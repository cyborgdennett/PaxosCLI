﻿using Microsoft.EntityFrameworkCore;
using PaxosCLI.Database;
using PaxosCLI.Messaging;

namespace PaxosCLI.NodeAgents;

/// <summary>
/// The Proposer is capable of conducting ballots.
/// It knows when a voting round was successful, and will notify the other nodes when such is the case.
/// </summary>
public class Proposer
{
    private Node _parentNode;
    private DateTime TimeAtPreviousAction;
    public static readonly string OLIVE_DAY_DECREE = "The ides of February is national olive day";
    public bool presidentInitTaskFinished;
    private Queue<byte[]> Proposals;
    private Queue<Tuple<string, byte[]>> TransactionProposals;
    private int totalQueeSize = 5;
    private Thread ExecutePaxosOnReceivedProposalThread;
    private Dictionary<String, Node> NetworkLeaderNodes { get; set; }
    private List<int> FoundTransactionIds { get; set; }
    private Queue<Tuple<string, int, byte[], Node>> TransactionQueue;//Received transactions
    Thread BeginProposingOnInputThread;

    public Proposer(Node node)
    {
        _parentNode = node;
        Proposals = new Queue<byte[]>();
        NetworkLeaderNodes = new();
        TransactionProposals = new Queue<Tuple<string, byte[]>>();
        TransactionQueue = new Queue<Tuple<string, int, byte[], Node>>();
    }

    /// <summary>
    /// The first string is the network name or empty if not an transaction, the second is the decree
    /// !!!Override this in the Program.cs for the application level
    /// </summary>
    /// <returns></returns>
    public Tuple<string, string> Input()
    {
        string input;
        if (_parentNode.hasSensor)
        {
            input = _parentNode.sensorData.Read().Trim();
            if (input == "") return null;
        }
        else
        {
            input = Console.ReadLine();
        }
        
        return new Tuple<string, string>("", input);
    }

    /// <summary>
    /// Start listening for input to proposer
    /// </summary>
    public void InitBeginProposingOnInput()
    {
        BeginProposingOnInputThread = new Thread(async () =>
        {
            while (true)
            {
                await BeginProposingOnInput();
            }
        });
        BeginProposingOnInputThread.Start();
    }
    
    /// <summary>
    /// Allows for sending proposals from the command line.
    /// If the current node is NOT the president, redirect the proposal to the president
    /// </summary>
    private async Task BeginProposingOnInput()
    {

        Console.WriteLine("[Proposer] Preparing...");
        while (!_parentNode.Client.IsSending && !_parentNode.Server.isListening
                || (_parentNode.isPresident && !presidentInitTaskFinished))
        {
            //If the current node is a president, wait for the inital president tasks to be finished
            Thread.Sleep(100);
        }

        //TODO: create system that can use different input functions
        while (_parentNode.Client.IsSending && _parentNode.Server.isListening)
        {
            Thread.Sleep(500);
            Tuple<string, string> inputtuple = null;
            if (_parentNode._messages.TryDequeue(out inputtuple))
            {

            }
            else
            {
                //inputtuple = Input();
            }
            if(inputtuple == null) continue;
            

            Console.WriteLine(inputtuple);
            byte[] inputInBytes = MessageHelper.StringToByteArray(inputtuple.Item2);

            if (_parentNode.isPresident)
            {
                if (inputtuple.Item1 == "")
                {
                    Console.WriteLine("sending to self");
                    Proposals.Enqueue(inputInBytes);
                }
                else
                {
                    TransactionProposals.Enqueue(new Tuple<string, byte[]>(inputtuple.Item1, inputInBytes));
                }
            }
            else if (!_parentNode.isPresident)
            {
                if (_parentNode.PresidentNode == null)
                {
                    Console.WriteLine("[Proposer] Waiting for president to be known...");
                    while (_parentNode.PresidentNode == null)
                    {
                        Thread.Sleep(100);
                    }
                }
                Message msg;

                //send the proposal to the president
                if (inputtuple.Item1 == "")
                    msg = new DecreeProposal(_parentNode.Client._messageIdCounter,
                                                                _parentNode.Id, inputInBytes);
                else
                    msg = new TransactionProposal(_parentNode.Client._messageIdCounter,
                                                                _parentNode.Id, inputtuple.Item1, inputInBytes);
                Console.WriteLine(msg);
                await _parentNode.Client.SendMessageToNode(msg, _parentNode.PresidentNode, true, true);
            }
        }
    }

    public async Task ProposeOnInput(Tuple<string, string> inputtuple)
    {
        while (!_parentNode.Client.IsSending && !_parentNode.Server.isListening
                || (_parentNode.isPresident && !presidentInitTaskFinished))
        {
            //If the current node is a president, wait for the inital president tasks to be finished
            Thread.Sleep(100);
        }

        if (inputtuple == null) return;

        Console.WriteLine(inputtuple.Item1);
        byte[] inputInBytes = MessageHelper.StringToByteArray(inputtuple.Item2);

        if (_parentNode.isPresident)
        {
            if (inputtuple.Item1 == "")
                Proposals.Enqueue(inputInBytes);
            else
            {
                TransactionProposals.Enqueue(new Tuple<string, byte[]>(inputtuple.Item1, inputInBytes));
            }
        }
        else if (!_parentNode.isPresident)
        {
            if (_parentNode.PresidentNode == null)
            {
                Console.WriteLine("[Proposer] Waiting for president to be known...");
                while (_parentNode.PresidentNode == null)
                {
                    Thread.Sleep(100);
                }
            }
            Message msg;

            //send the proposal to the president
            if (inputtuple.Item1 == "")
                msg = new DecreeProposal(_parentNode.Client._messageIdCounter,
                                                            _parentNode.Id, inputInBytes);
            else
                msg = new TransactionProposal(_parentNode.Client._messageIdCounter,
                                                            _parentNode.Id, inputtuple.Item1, inputInBytes);
            await _parentNode.Client.SendMessageToNode(msg, _parentNode.PresidentNode, true, true);
        }
    }

    /// <summary>
    /// Whenever a round of Paxos needs to be started (trying to get a decree written), this function needs to be called.
    /// </summary>
    /// <param name="proposedDecree">The decree to write to the ledger</param>
    /// <param name="isFill">If the decree is an unimportant (olive-day) decree</param>
    /// <param name="isNewDecree">If the decree is a new decree (not a learned decree)</param>
    /// <param name="entryId">The id of the decree</param>
    /// <returns></returns>
    public async Task ExecutePaxos(byte[] proposedDecree, bool isFill = false, bool isNewDecree = false, long entryId = 0)
    {
        Cluster quorum = GetOnlineNodes();
        bool initFinished = ((!isNewDecree && !presidentInitTaskFinished) || presidentInitTaskFinished);

        if (_parentNode.isPresident
            && _parentNode.status == NodeStatus.idle
            && initFinished)
        {
            Console.WriteLine("\n[Proposer] Executing Paxos");

            int ballotSuccessful = 1;
            do
            {
                quorum = GetOnlineNodes();
                _parentNode.status = NodeStatus.trying;
                ballotSuccessful = await StartPollingMajoritySet(quorum, proposedDecree, isFill, isNewDecree, entryId);
            } while (ballotSuccessful == 1);

            if (ballotSuccessful == 0)
            {
                await Succeed();
            }
            else if (ballotSuccessful == 2)
            {
                Console.WriteLine("[Proposer] Aborting Paxos");
            }
        }
        else if (!initFinished)
        {
            Console.WriteLine("[Proposer] Not ready to conduct ballot.");
        }
    }

    /// <summary>
    /// Do paxos untill all proposals are worked out in a looping fashion. This uses the message Success and BeginBallot in one untill you reach the last proposal.
    /// </summary>
    /// <param name="proposedDecree">The decree to write to the ledger</param>
    /// <param name="isFill">If the decree is an unimportant (olive-day) decree</param>
    /// <param name="isNewDecree">If the decree is a new decree (not a learned decree)</param>
    /// <param name="entryId">The id of the decree</param>
    /// <returns></returns>
    public async Task ExecutePaxosLoop(byte[] proposedDecree, bool isFill = false, bool isNewDecree = false, long entryId = 0)
    {
        byte[] success = null;
        int ballotSuccessful = 1;
        while (Proposals.Count() >= 1)
        {
            Cluster quorum = GetOnlineNodes();
            bool initFinished = ((!isNewDecree && !presidentInitTaskFinished) || presidentInitTaskFinished);

            if (_parentNode.isPresident
                && _parentNode.status == NodeStatus.idle
                && initFinished)
            {
                Console.WriteLine("\n[Proposer] Executing CompressedPaxos");


                if (ballotSuccessful == 2)
                {
                    Console.WriteLine("[Proposer] Aborting CompressedPaxos");
                    _parentNode.CompressedPaxos = false;
                    return;
                }
                //This is only used for the first iteration
                else if (success == null)
                {
                    do
                    {
                        quorum = GetOnlineNodes();
                        _parentNode.status = NodeStatus.trying;
                        ballotSuccessful = await StartPollingMajoritySet(quorum, proposedDecree, isFill, isNewDecree, entryId);
                    } while (ballotSuccessful == 1);
                    _parentNode.CompressedPaxos = true;
                }
                else if (ballotSuccessful == 0 && await Succeed() == 1)
                {
                    success = await _parentNode.LedgerHelper.GetOutcome(_parentNode.entryId);
                    if (success == null)
                        return;

                    proposedDecree = Proposals.Dequeue();
                    //send a SuccessBeginBallotMessage
                    do
                    {
                        quorum = GetOnlineNodes();
                        _parentNode.status = NodeStatus.trying;
                        ballotSuccessful = await StartPollingMajoritySet(quorum, proposedDecree, isFill, isNewDecree, entryId, success, _parentNode.entryId);
                    } while (ballotSuccessful == 1);
                }
            }
            else if (!initFinished)
            {
                Console.WriteLine("[Proposer] Not ready to conduct ballot.");
                return;
            }
        }
    }

    public async Task ExecuteCrossNetwork(decimal decreeId, string network)
    {
        //Check if decree is known
        byte[] decree = await _parentNode.LedgerHelper.GetOutcome((long)decreeId);
        if(decree == null)
        {
            Console.WriteLine("[Proposer] Transaction Error: Decree unknown");
            return;
        }
        //if you are at your own networkname, you can stop the procedure and send a success message to the other Leader
        if (_parentNode.NetworkName == network)
        {
            Console.WriteLine("[Proposer] Transaction Error: Cannot send transaction to own network");
            return;
        }
        string NETWORK_FILE_PATH = "Nodes/" + network + ".csv";
        Stack<string> endpoints;
        try
        {
            endpoints = new Stack<string>(File.ReadAllLines(NETWORK_FILE_PATH));
        }
        catch (Exception e) 
        { 
            Console.WriteLine("[Proposer] Transaction Error: Network " + NETWORK_FILE_PATH + " not found"); 
            return; 
        }
        if (endpoints.Count == 0)
        {
            Console.WriteLine("[Proposer] Transaction Error: Network " + NETWORK_FILE_PATH + " file empty");
            return;
        }

        //Make an cluster of the csv file
        Cluster TransactionNodes = new();
        foreach (String endpoint in endpoints)
        {

            string[] endpointPropterties = endpoint.Split(',');
            int e_id = Int32.Parse(endpointPropterties[0]);
            System.Net.IPAddress e_ip = System.Net.IPAddress.Parse(endpointPropterties[1]);
            int e_port = Int32.Parse(endpointPropterties[2]);
            Node e_node = new Node(e_id, e_ip, e_port);

            TransactionNodes.TryAdd(e_id, e_node);
        }

        Random rand = new();
        //create random transactionId
        int transactionId = rand.Next(0, int.MaxValue); 

        NetworkLeaderNodes.Remove(network);
        //make "Find Leader"-message and send to the cluster
        FindLeader findLeaderMsg = new(_parentNode.Client._messageIdCounter, _parentNode.Id, network);
        await _parentNode.Client.SendMessageToCluster(findLeaderMsg,TransactionNodes,false);
        DateTime t = DateTime.Now;
        bool found = false;
        while ((DateTime.Now - t).TotalSeconds < 2.0)
        {
            if (NetworkLeaderNodes.ContainsKey(network))
            {
                found = true;

                Transaction transactionMsg = new(_parentNode.Client._messageIdCounter, _parentNode.Id, network, transactionId, decree);
                await _parentNode.Client.SendMessageToNode(transactionMsg, NetworkLeaderNodes[network], false, false);
            }
        }
        if (!found)
        {
            Console.WriteLine("[Proposer] Transaction Error: FindLeader timeout");
            return;
        }
        t = DateTime.Now;
        found = false;
        while ((DateTime.Now - t).TotalSeconds < 2.0)
        {
            if (FoundTransactionIds.Contains(transactionId))
            {
                Console.WriteLine("[Proposer] Transaction [{0]] complete",transactionId);
            }
        }     
        if (!found)
        {
            Console.WriteLine("[Proposer] Transaction Error: TransactionSuccess timeout");
            return;
        }
    }

    public async Task OnLeader(Leader leader)
    {
        try
        {
            System.Net.IPAddress ip = System.Net.IPAddress.Parse(leader._ip.Substring(0, leader._ip.IndexOf(':')));
            int port = int.Parse(leader._ip.Substring(leader._ip.IndexOf(':')+1));   
            Node leaderNode = new Node(leader._nodeId, ip, port);
            NetworkLeaderNodes[leader._networkName] = leaderNode;
            Console.WriteLine("[Proposer] added node [{0}] as leader of network [{1}]",leader._nodeId, leader._networkName);
        } 
        catch (Exception ex) { Console.WriteLine("[Proposer] received invalid IP");}

    }

    public async Task OnTransactionSuccess(TransactionSuccess message)
    {
        FoundTransactionIds.Append(message._transactionId);
        Console.WriteLine("[Proposer] Received FoundTransaction message");
    }

    public async Task OnTransaction(Transaction transaction, Node node) 
    { 
        TransactionQueue.Enqueue(new Tuple<string, int, byte[], Node>(transaction._networkName, transaction._transactionId, transaction._decree, node));
        Console.WriteLine("[Proposer] Received transaction message");
    }

    public async Task<int> TryNewBallot()
    {
        if (_parentNode.isPresident)
        {
            _parentNode.status = NodeStatus.trying;
            _parentNode.prevVotes = new List<LastVote>();
            return await SendNextBallotMessage();
        }
        return 2;
    }

    public async Task<int> SendNextBallotMessage()
    {
        TimeAtPreviousAction = DateTime.Now;
        Cluster setOfNodes = GetOnlineNodes();
        bool majorityOnline = setOfNodes.HasMajorityOf(_parentNode.AllNodes);

        if (_parentNode.status == NodeStatus.trying
            && _parentNode.isPresident
            && majorityOnline)
        {
            Console.WriteLine("[Proposer] Sending nextBallotMsg to {0},", String.Join(",", setOfNodes.Keys));
            TimeAtPreviousAction = DateTime.Now;
            long lastEntryUntilMissing = await _parentNode.LedgerHelper.GetLastEntryIdUntilMissingData();
            NextBallot nextBallot = new NextBallot(_parentNode.Client._messageIdCounter, _parentNode.Id, _parentNode.lastTried, lastEntryUntilMissing);

            //send message to peers
            await _parentNode.Client.SendMessageToCluster(nextBallot, setOfNodes.GetClusterExcludingNode(_parentNode), true);


            //send this message to own acceptor
            await _parentNode.Acceptor.OnReceiveNextBallot(nextBallot);

            //wait for reply from set of nodes
            while (_parentNode.prevVotes.Count() != setOfNodes.Count())
            {
                if ((DateTime.Now - TimeAtPreviousAction).TotalMilliseconds >= Node.MINUTE_IN_PAXOS_TIME * 22)
                {
                    Console.WriteLine("[Proposer] Didn't get enough lastvote messages in time.");
                    return 1;
                }
            }
            Console.WriteLine("[Proposer] Received all lastvotes from chosen set of nodes.");
            return 0;
        }
        else if (!majorityOnline)
        {
            return 1;
        }
        else if (!_parentNode.isPresident)
        {
            Console.WriteLine("[Proposer] Cannot send a NextBallot message as a non-president.");
        }
        else if (_parentNode.status != NodeStatus.trying)
        {
            Console.WriteLine("[Proposer] Cannot send a NextBallot message when not trying.");
        }
        return 2;
    }


    public void ReceiveLastVoteMessage(LastVote lastVote)
    {
        if (lastVote._nextBal == _parentNode.lastTried && _parentNode.status == NodeStatus.trying)
        {
            Console.WriteLine("[Proposer] Received lastvote from node {0}.", lastVote._senderId);
            _parentNode.prevVotes.Add(lastVote);
        }
    }

    /// <summary>
    /// If a president learns about decrees from non-presidents, ballots have to be conducted for these decrees.
    /// </summary>
    /// <returns></returns>
    public async Task ConductBallotsMissingDecrees()
    {
        //learn about any missing decrees sent by lastvote from other priests
        if (_parentNode.isPresident && _parentNode.prevVotes.Count() >= 1)
        {
            List<LedgerEntry> entries = await _parentNode.LedgerHelper.GetEntries();
            HashSet<long> decreesInLedger = new HashSet<long>();
            List<string> missingDecreesStrings =
                _parentNode.prevVotes.Select(v => v._missingDecrees)
                .Where(s => s.Length > 0).ToList();
            Dictionary<long, byte[]> decreesToPropose = new Dictionary<long, byte[]>();

            foreach (string missingDecreesString in missingDecreesStrings)
            {
                string[] missingDecrees = missingDecreesString.Split('|');

                foreach (var missingDecree in missingDecrees)
                {
                    string[] entryInformation = missingDecree.Split(':');
                    long entryId = long.Parse(entryInformation[0]);
                    byte[] decree = MessageHelper.StringToByteArray(entryInformation[1]);

                    //if not sure in ledger
                    if (!decreesInLedger.Contains(entryId))
                    {
                        LedgerEntry entryInDb = entries.SingleOrDefault(e => e.Id == entryId);
                        if (entryInDb != null && !entryInDb.Decree.Equals(OLIVE_DAY_DECREE))
                        {
                            //just discovered is in ledger
                            //also updates any olive day decrees
                            decreesInLedger.Add(entryId);
                            continue;
                        }
                    }
                    else// if decree known in ledger
                    {
                        continue;
                    }

                    if (!decreesToPropose.ContainsKey(entryId))
                    {
                        decreesToPropose.Add(entryId, decree);
                    }
                }
            }

            foreach (var decreeToPropose in decreesToPropose)
            {
                //not in ledger, so needs to be proposed/written
                Console.WriteLine("\n[Proposer] Learned about [{0}:{1}]. Conducting ballot.",
                                    decreeToPropose.Key,
                                    MessageHelper.ByteArrayToString(decreeToPropose.Value));
                _parentNode.entryId = decreeToPropose.Key;
                _parentNode.decree = decreeToPropose.Value;
                await ExecutePaxos(decreeToPropose.Value, false, false, decreeToPropose.Key);
            }
        }
    }


    /// <summary>
    /// Informs a non-president with its missing decrees.
    /// The selected decrees should not exist in the non-president's ledger
    /// and should be equal to or lower than the number of the decree written in the president't ledger 
    /// up until it is missing decrees.
    /// </summary>
    /// <param name="rmem"></param>
    /// <returns></returns>
    public async Task InformMissingDecrees(RequestMissingEntriesMessage rmem)
    {
        List<long> missingDecreeIds =
            rmem._entriesInOwnLedgerString.Length > 0
            ? rmem._entriesInOwnLedgerString.Split('|').Select(d => long.Parse(d)).ToList()
            : new List<long>();

        string entriesToInformString = await _parentNode.LedgerHelper.GetMissingEntriesForNonPresident(missingDecreeIds);
        InformMissingEntriesMessage informMissingEntriesMessage =
            new InformMissingEntriesMessage(_parentNode.Client._messageIdCounter,
                                            _parentNode.Id,
                                            entriesToInformString);

        if (entriesToInformString.Length > 0)
        {
            Console.WriteLine("[Proposer] Informing {0} with missing decrees: [{1}]", rmem._senderId, entriesToInformString);
            await _parentNode.Client.SendMessageToNode(informMissingEntriesMessage, rmem._senderId, true, true);
        }
    }


    /// <summary>
    /// Begins a ballot for a specified quorum, with a specified decree and decree id
    /// </summary>
    /// <param name="quorum">Collection of nodes, part of the voting process</param>
    /// <param name="proposedDecree">The decree attempted to be written to the distributed ledger</param>
    /// <param name="isFill">If the current decree is for filling with olive-day decrees</param>
    /// <param name="isNewDecree">If the current decree is a decree not written in the ledger yet</param>
    /// <param name="entryId">The decree id</param>
    /// <returns></returns>
    private async Task<int> StartPollingMajoritySet(Cluster quorum, byte[] proposedDecree, bool isFill, bool isNewDecree, long entryId = 0, byte[]? success = null, long? successId = null)
    {
        //TODO this doesn't work if nodes leave the network (due to prevVote change).
        //although, the part-time parliament expects no node one to join or leave during operation.
        //This is the main problem mentioned in the thesis. The prevVotes set stays static while the quorum is dynamic.
        //A good look needs to be taken at how the quorum is formed.
        bool quorumMembersAreLastVoters =
            quorum.Keys.ToList()
            .Intersect(_parentNode.prevVotes.Select(lv => lv._senderId).ToList())
            .Count() == quorum.Count();

        if (_parentNode.isPresident
            && _parentNode.status == NodeStatus.trying
            && quorumMembersAreLastVoters)
        {
            Console.WriteLine("[Proposer] Polling...");
            _parentNode.status = NodeStatus.polling;
            _parentNode.quorum = quorum;
            _parentNode.voters = new Cluster();

            _parentNode.isFill = isFill;
            _parentNode.isNewDecree = isNewDecree;

            if (entryId != 0) //if entryId has been given
            {
                _parentNode.entryId = entryId;
            }
            else //if entryId has not been given
            {
                if (!isFill) //when it's a normal entry
                {
                    LedgerEntry lastEntryInDb = null;

                    using (Ledger ledger = new Ledger())
                    {
                        lastEntryInDb = await ledger.Entries.OrderBy(l => l.Id).LastOrDefaultAsync();
                    }

                    if (lastEntryInDb != null)
                    {
                        _parentNode.entryId = lastEntryInDb.Id;
                        _parentNode.entryId++;
                    }
                    else
                    {
                        _parentNode.entryId = 1;
                    }

                    if (isNewDecree)
                    {
                        _parentNode.decree = proposedDecree;
                    }
                    else
                    {
                        _parentNode.decree = GetDecreeToPropose(proposedDecree);
                    }
                }
                else //when it's filling with olive day decrees
                {
                    _parentNode.decree = proposedDecree;
                }
            }


            if (success == null || successId == null)
            {
                Console.WriteLine("[Proposer] Ballot: decreeId={0}, decree={1}, quorum={2}", _parentNode.entryId, MessageHelper.ByteArrayToString(_parentNode.decree), String.Join(",", quorum.Keys.ToArray()));
                return await SendBeginBallotMessage();
            }
            else
            {
                Console.WriteLine("[Proposer] Success: decreeId={0}, decree={1}", successId, success);
                Console.WriteLine("[Proposer] BeginBallot: decreeId={0}, decree={1}, quorum={2}", _parentNode.entryId, MessageHelper.ByteArrayToString(_parentNode.decree), String.Join(",", quorum.Keys.ToArray()));
                return await SendSuccessBeginBallotMessage((long)successId, success);
            }
        }
        return 1;
    }

    /// <summary>
    /// Send a success and beginballot msg in one.
    /// !!!DOES NOT WORK!!!
    /// </summary>
    /// <returns></returns>
    private async Task<int> SendSuccessBeginBallotMessage(long successId, byte[] success)
    {

        if (_parentNode.status == NodeStatus.polling && _parentNode.isPresident)
        {
            TimeAtPreviousAction = DateTime.Now;

            Console.WriteLine("[Proposer] Sending passed decree [{0}:{1}] and NextBallot [{2}:{3}] to learners.", successId, MessageHelper.ByteArrayToString(success), _parentNode.lastTried, _parentNode.decree);
            SuccessBeginBallot successBeginBallotMsg = new SuccessBeginBallot(_parentNode.Client._messageIdCounter, _parentNode.Id, success, successId, _parentNode.lastTried, _parentNode.decree);
            await _parentNode.Client.SendMessageToCluster(successBeginBallotMsg, _parentNode.quorum.GetClusterExcludingNode(_parentNode), true);

            //send this message to own acceptor
            await _parentNode.Acceptor.OnReceiveSuccessBeginBallot(successBeginBallotMsg);

            while (_parentNode.voters.Count() < _parentNode.quorum.Count())
            {
                //wait for every quorum member to reply
                if ((DateTime.Now - TimeAtPreviousAction).TotalMilliseconds >= Node.MINUTE_IN_PAXOS_TIME * 22)
                {
                    return 1;
                }
            }
            return 0;
        }
        else
        {
            Console.WriteLine("[Proposer] Cannot send beginballot message, because not polling.");
        }
        return 1;
    }

    private async Task<int> SendBeginBallotMessage()
    {
        if (_parentNode.status == NodeStatus.polling && _parentNode.isPresident)
        {
            TimeAtPreviousAction = DateTime.Now;
            BeginBallot beginBallotMsg = new BeginBallot(_parentNode.Client._messageIdCounter, _parentNode.Id, _parentNode.lastTried, _parentNode.decree);
            await _parentNode.Client.SendMessageToCluster(beginBallotMsg, _parentNode.quorum.GetClusterExcludingNode(_parentNode), true);

            //send this message to own acceptor
            await _parentNode.Acceptor.OnReceiveBeginBallot(beginBallotMsg);

            while (_parentNode.voters.Count() < _parentNode.quorum.Count())
            {
                //wait for every quorum member to reply
                if ((DateTime.Now - TimeAtPreviousAction).TotalMilliseconds >= Node.MINUTE_IN_PAXOS_TIME * 22)
                {
                    return 1;
                }
            }
            return 0;
        }
        else
        {
            Console.WriteLine("[Proposer] Cannot send beginballot message, because not polling.");
        }
        return 1;
    }

    public void ReceiveVotedMessage(Voted voted)
    {
        if (voted._ballotId == _parentNode.lastTried && _parentNode.status == NodeStatus.polling)
        {
            Console.WriteLine("[Proposer] Received voted message from {0} for {1}.", voted._senderId, voted._ballotId);
            _parentNode.voters.TryAdd(voted._senderId, _parentNode.AllNodes.GetNodeById(voted._senderId));
        }
        else if (voted._ballotId != _parentNode.lastTried)
        {
            Console.WriteLine("[Proposer] Received a voted message from a different kind of ballot. Vote ballotid={0}. lastTried={1}", voted._ballotId, _parentNode.lastTried);
        }
        else if (_parentNode.status != NodeStatus.polling)
        {
            Console.WriteLine("[Proposer] This node is not polling. Not doing anything with voted message.");
            Console.WriteLine("[Proposer] Status = {0}", _parentNode.status);
        }
    }

    /// <summary>
    ///   Checks if the decree has been voted for by the quorum.
    ///   If so, it writes it to the ledger and sends a success message to all other peers
    /// </summary>
    private async Task<int> Succeed()
    {
        TimeAtPreviousAction = DateTime.Now;
        bool quorumMembersAreVoters =
            _parentNode.quorum.Keys.ToList()
            .Intersect(_parentNode.voters.Keys.ToList())
            .Count() == _parentNode.quorum.Count();


        byte[] outcome = await _parentNode.LedgerHelper.GetOutcome(_parentNode.entryId);

        if (_parentNode.status == NodeStatus.polling
            && quorumMembersAreVoters
            && outcome == null || (outcome != null && MessageHelper.ByteArrayToString(outcome).Equals(OLIVE_DAY_DECREE)))
        {
            Console.WriteLine("[Proposer] Ballot [{0}:{1}] succeeded.",
                                _parentNode.entryId,
                                MessageHelper.ByteArrayToString(_parentNode.decree));

            Success success = new Success(_parentNode.Client._messageIdCounter,
                                            _parentNode.Id,
                                            _parentNode.decree,
                                            _parentNode.entryId);



            await _parentNode.Learner.WriteSingleDecreeToLedgerImmediately(success);

            _parentNode.status = NodeStatus.idle;

            //check if compressedPaxos is on
            if (!_parentNode.CompressedPaxos)
            {
                await SendSuccessMessage(success);
            }
            return 1;
        }
        else if (!quorumMembersAreVoters)
        {
            Console.WriteLine("[Proposer] Not succeeding ballot. Not all quorum members voted.");
        }
        else if (_parentNode.status != NodeStatus.polling)
        {
            Console.WriteLine("[Proposer] Not succeeding ballot, Node is not polling.");
        }
        else if (outcome != null)
        {
            Console.WriteLine("[Proposer] Not succeeding ballot, Outcome is already known: {0}", MessageHelper.ByteArrayToString(outcome));
        }
        _parentNode.status = NodeStatus.idle;
        return -1;
    }

    /// <summary>
    /// Ballot was successful, and so sends outcome to other nodes
    /// </summary>
    /// <param name="success">Message containing infromation of the decree to write.</param>
    private async Task SendSuccessMessage(Success success)
    {
        byte[] outcome = await _parentNode.LedgerHelper.GetOutcome(_parentNode.entryId);
        if (outcome != null)
        {
            Console.WriteLine("[Proposer] Sending passed decree [{0}:{1}] to learners.", _parentNode.entryId, MessageHelper.ByteArrayToString(outcome));
            await _parentNode.Client.SendMessageToCluster(success, _parentNode.Peers, true);
        }
        else
        {
            Console.WriteLine("[Proposer] Not sending success message, since outcome is not known.");
        }
    }

    private Cluster GetOnlineNodes()
    {
        Cluster quorum = new Cluster(_parentNode.OnlinePeers);
        quorum.TryAdd(_parentNode.Id, _parentNode);
        return quorum;
    }

    /// <summary>
    /// Gets the decree to propose based on requirement 3 (B3 from Part-Time Parliament)
    /// </summary>
    /// <param name="proposedDecree">The decree proposed for this ballot</param>
    private byte[] GetDecreeToPropose(byte[] proposedDecree)
    {
        foreach (LastVote lv in _parentNode.prevVotes)
        {
            Console.WriteLine(lv.ToString());
        }

        LastVote highestLastVote = _parentNode.prevVotes.OrderByDescending(v => v._prevBal).First();

        if (highestLastVote._prevBal != decimal.MinValue)
        {
            return highestLastVote._prevDecree;
        }
        else
        {
            return proposedDecree;
        }
    }

    /// <summary>
    /// The steps required to take once a node becomes president
    /// </summary>
    public async Task OnBecomingPresident()
    {
        if (!presidentInitTaskFinished && _parentNode.isPresident)
        {
            Console.WriteLine("[Proposer] Learning decrees...");
            int newBallotMsgResult = 1;
            await IncrementBallotId();

            do
            {
                //execute step 1-2 to learn about decrees and prepare for steps 3-6
                newBallotMsgResult = await TryNewBallot();
            } while (newBallotMsgResult != 0 && _parentNode.isPresident);

            if (newBallotMsgResult == 0)
            {
                _parentNode.status = NodeStatus.idle;
                await ConductBallotsMissingDecrees();
                await FillGapsInLedger();
                presidentInitTaskFinished = true;
                InitExecutePaxosWhenProposerTask();
            }
        }
    }

    /// <summary>
    /// Execute Paxos whenever a decree is required to be written to the ledger (see collection: proposals)
    /// </summary>
    private void InitExecutePaxosWhenProposerTask()
    {
        ExecutePaxosOnReceivedProposalThread = new Thread(async () =>
                {
                    while (presidentInitTaskFinished)
                    {

                        //This part is for transactions proposals from own to other networks
                        if (TransactionProposals.Count() > 0
                            && _parentNode.status == NodeStatus.idle
                            && GetOnlineNodes().HasMajorityOf(_parentNode.AllNodes))
                        {
                            Tuple<string, byte[]> toBallot = TransactionProposals.Dequeue();
                            await ExecutePaxos(toBallot.Item2, false, true, 0);


                            //Check if the ballot is passed and written in the ledger
                            if (_parentNode.prevDec == toBallot.Item2)
                            {
                                Console.WriteLine("Transaction has been written in ledger, sending to [{0}]",toBallot.Item1);
                                //send the transaction to the other network
                                await ExecuteCrossNetwork(_parentNode.prevBal, toBallot.Item1);
                            }

                        }

                        //This part is for received Transactions from other networks
                        if(TransactionQueue.Count() > 0
                            && _parentNode.status == NodeStatus.idle
                            && GetOnlineNodes().HasMajorityOf(_parentNode.AllNodes))
                        {
                            Tuple<string, int, byte[], Node> toBallot = TransactionQueue.Dequeue();
                            await ExecutePaxos(toBallot.Item3, false, true, 0);

                            //Check if the ballot is passed and written in the ledger
                            if (_parentNode.prevDec == toBallot.Item3)
                            {
                                Console.WriteLine("Transaction [{0}] from network [{1}] has been written in the ledger", toBallot.Item2, toBallot.Item1);
                                //Give notice to sender network
                                TransactionSuccess message = new(_parentNode.Client._messageIdCounter, _parentNode.Id, toBallot.Item1,toBallot.Item2);
                                await _parentNode.Client.SendMessageToNode(message, toBallot.Item4, false, false);
                            }
                        }


                        //This part is for Decrees
                        if (Proposals.Count() > 0
                            && _parentNode.status == NodeStatus.idle
                            && GetOnlineNodes().HasMajorityOf(_parentNode.AllNodes))
                        {

                            if (Proposals.Count() == 1)
                            {
                                byte[] toBallot = Proposals.Dequeue();
                                await ExecutePaxos(toBallot, false, true, 0);
                            }
                            else
                            {
                                byte[] toBallot = Proposals.Dequeue();
                                await ExecutePaxosLoop(toBallot, false, true, 0);
                            }
                        }
                    }
                });
        ExecutePaxosOnReceivedProposalThread.Start();
    }

    /// <summary>
    /// Fills missing decrees with unimportant decrees by attempting to pass ballots
    /// </summary>
    public async Task FillGapsInLedger()
    {
        List<int> missingEntryIds;

        if (_parentNode.isPresident)
        {
            List<LedgerEntry> entries = await _parentNode.LedgerHelper.GetEntries();
            List<int> writtenEntryIds = entries.Select(e => (int)e.Id).ToList();

            if (writtenEntryIds.Any())
            {
                missingEntryIds = Enumerable.Range(1, (int)writtenEntryIds.Last()).Except(writtenEntryIds).ToList();

                foreach (int entryId in missingEntryIds)
                {
                    if (_parentNode.isPresident)
                    {
                        Console.WriteLine("\n[Proposer] Attempting to fill decree {0} with olive day decree.", entryId);
                        _parentNode.entryId = entryId;
                        _parentNode.decree = MessageHelper.StringToByteArray(OLIVE_DAY_DECREE);
                        await ExecutePaxos(_parentNode.decree, true, false, entryId);
                    }
                    else
                    {
                        return;
                    }
                }

                if (missingEntryIds.Count() > 0)
                {
                    Console.WriteLine("[Proposer] Filled gaps in ledger with olive day decree.");
                }
            }
            else
            {
                Console.WriteLine("[Proposer] No gaps to fill with olive day decrees.");
            }
        }
    }

    /// <summary>
    /// Whenever the president is notified with a nextBal, update lasttried and execute Paxos
    /// THIS METHOD IS NOT FINISHED. How does one abort a current execution of paxos?
    /// </summary>
    /// <param name="nextBal">The updated ballot number</param>
    /// <returns></returns>
    public async Task ReceiveNewerBallotNumber(decimal nextBal)
    {
        //update ballot number
        _parentNode.lastTried = MessageHelper.CreateUniqueMessageId((long)Math.Truncate(nextBal), _parentNode.Id);
        // TODO how to stop the current Paxos execution, and restart with newer ballot number?
        //await ExecutePaxos(_parentNode.decree,
        //                   _parentNode.isFill,
        //                   _parentNode.isNewDecree,
        //                   _parentNode.entryId);
    }

    public void OnDecreeProposal(DecreeProposal decreeProposal)
    {
        Proposals.Enqueue(decreeProposal._decree);
    }

    /// <summary>
    /// TODO: make sure that after it is in the own network, it will be send to the other network
    /// </summary>
    /// <param name="decreeProposal"></param>
    public void OnTransactionProposal(TransactionProposal transactionProposal)
    {
        TransactionProposals.Enqueue(new Tuple<string, byte[]>(transactionProposal._networkName, transactionProposal._decree));
    }

    private async Task IncrementBallotId()
    {
        if (_parentNode.lastTried == decimal.MinValue)
        {
            _parentNode.lastTried = MessageHelper.CreateUniqueMessageId(1, _parentNode.Id);
        }
        else
        {
            _parentNode.lastTried++;
        }

        await _parentNode.LedgerHelper.SavePaxosProgressAsync(_parentNode);
        Console.WriteLine("[Proposer] Ballot id incremented to: {0}", _parentNode.lastTried);
    }
}

