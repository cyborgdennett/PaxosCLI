using PaxosCLI.Messaging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PaxosCLI.NodeAgents;
public class NetworkTransaction
{
    public Node _parentNode { private set; get; }
    private ConcurrentDictionary<String, Node> NetworkLeaderNodes;
    private SemaphoreSlim _messagesAvailable = new SemaphoreSlim(0);
    private ConcurrentDictionary<int, bool> FoundTransactionIds;
    private ConcurrentQueue<(string network, byte[] decree, bool saveToLedger)> TransactionProposals;
    private ConcurrentQueue<(string network, int transId, byte[] decree, Node node)> TransactionQueue;
    private HashSet<string> KnownNetworks;
    public bool SaveTransactionToLedger { private set; get; }//set to True to allow saving to ledger before sending to other network
    private Thread? TransactionHandlerThread;
    private Thread? ReceiveTransactionThread;

    public NetworkTransaction(Node node, bool saveTransactionToLedger = false)
    {
        _parentNode = node;
        SaveTransactionToLedger = saveTransactionToLedger;

        TransactionProposals = new();
        TransactionQueue = new();
        FoundTransactionIds = new();
        NetworkLeaderNodes = new();
        KnownNetworks = new();

        //fill list of networks
        GetNetworkNames();

        InitNetworkTransaction();
        InitReceiveNetworkTransaction();
    }
    public void AddTransaction(string network, string decree, bool saveToLedger = false)
    {
        TransactionProposals.Enqueue(new(network, MessageHelper.StringToByteArray(decree), saveToLedger));
    }
    

    private void InitReceiveNetworkTransaction()
    {
        ReceiveTransactionThread = new Thread(async () =>
        {
            while (true)
            {
                if(TransactionQueue.Count > 0)
                {
                    if (!TransactionQueue.TryDequeue(out var transaction))
                        continue;
                    if (transaction.network != _parentNode.NetworkName)
                        continue;

                    Console.WriteLine("[Transaction] Received Id[{0}] with decree[{1}]", transaction.transId, MessageHelper.ByteArrayToString(transaction.decree));
                    //send transaction to paxos
                    _parentNode.ManualInput(MessageHelper.ByteArrayToString(transaction.decree));

                    DateTime t = DateTime.Now;
                    while (_parentNode.prevDec != transaction.decree && (DateTime.Now - t).TotalSeconds < 1 )
                    {
                        Thread.Sleep(10);
                    }
                    TransactionSuccess msg = new(_parentNode.Client._messageIdCounter, _parentNode.Id, _parentNode.NetworkName, transaction.transId);
                    await _parentNode.Client.SendMessageToNode(msg, transaction.node, false, false);
                    Console.WriteLine("[Transaction] Finished transaction Id[{0}] with decree[{1}]", transaction.transId, MessageHelper.ByteArrayToString(transaction.decree));


                }
                Thread.Sleep(10);
            }
        });
        ReceiveTransactionThread.Start();
    }


    private void InitNetworkTransaction()
    {
        TransactionHandlerThread = new Thread(async () =>
        {
            while (true)
            {
                if (TransactionProposals.Count > 0
                && TransactionProposals.TryDequeue(out var transaction)
                && KnownNetworks.Contains(transaction.network))
                {
                    //if you are at your own networkname, you can stop the procedure
                    if (_parentNode.NetworkName == transaction.network)
                    {
                        Console.WriteLine("[Transaction] Error: Cannot send transaction to own network");
                        continue;
                    }
                    if (_parentNode.isPresident)
                    {
                        if (SaveTransactionToLedger && transaction.saveToLedger)
                        {
                            //send decree to main paxos loop.
                            _parentNode.ManualInput(MessageHelper.ByteArrayToString(transaction.decree));

                            //wait for prevDec to be equal to this decree.
                            DateTime t = DateTime.Now;
                            while (_parentNode.prevDec != transaction.decree && (DateTime.Now - t).TotalSeconds < 1)
                            {
                                Thread.Sleep(10);
                            }
                        }
                        await ExecuteCrossNetwork(transaction.network, transaction.decree);
                    }
                    else
                    {
                        if (_parentNode.PresidentNode == null)
                        {
                            Console.WriteLine("[Transaction] Waiting for president to be known...");
                            while (_parentNode.PresidentNode == null)
                            {
                                Thread.Sleep(100);
                            }
                        }

                        //send transaction proposal to president
                        var msg = new TransactionProposal(_parentNode.Client._messageIdCounter,
                                                                _parentNode.Id, transaction.network, transaction.decree);
                        await _parentNode.Client.SendMessageToNode(msg, _parentNode.PresidentNode, false, false);
                    }
                }
                
                Thread.Sleep(1);
            }
        });
        TransactionHandlerThread.Start();
    }

    /// <summary>
    /// This function starts the process of an transaction between two networks
    /// </summary>
    /// <param name="network"></param>
    /// <param name="decree"></param>
    /// <returns></returns>
    private async Task ExecuteCrossNetwork(string network, byte[] decree)
    {
        //Get Cluster from given network
        var TransactionNodes = GetCluster(network);
        if (TransactionNodes == null) return;

        //delete possible previous instance of leader
        NetworkLeaderNodes.TryGetValue(network, out var value);
        NetworkLeaderNodes.TryRemove(new(network, value));

        //make "Find Leader"-message and send to the cluster
        FindLeader findLeaderMsg = new(_parentNode.Client._messageIdCounter, _parentNode.Id, network);

        foreach ((int i, Node node) in TransactionNodes)
        {
            Console.WriteLine("[Transaction] Sending to: {0}, from {1}", node.PortNumber, _parentNode.PortNumber);
            await _parentNode.Client.SendMessageToNode(findLeaderMsg, node, false, false);
        }

        DateTime t = DateTime.Now;
        bool found = false;
        while ((DateTime.Now - t).TotalSeconds < 1)
        {
            if (NetworkLeaderNodes.TryGetValue(network, out var networkLeader))
            {
                if (networkLeader == null)
                {
                    Console.WriteLine("[Transaction] Error: Leader Null");
                    return;
                }
                found = true;
                break;
            }
        }
        if (!found)
        {
            Console.WriteLine("[Transaction] Error: FindLeader timeout");
            return;
        }

        //create random transactionId
        Random rand = new();
        int transactionId = rand.Next(0, Int16.MaxValue);

        //send transaction message to leader
        Transaction transactionMsg = new(_parentNode.Client._messageIdCounter, _parentNode.Id, network, transactionId, decree);
        await _parentNode.Client.SendMessageToNode(transactionMsg, NetworkLeaderNodes[network], false, false);

        t = DateTime.Now;
        found = false;
        while ((DateTime.Now - t).TotalSeconds < 2)
        {
            if (FoundTransactionIds.TryGetValue(transactionId, out found))
            {
                if (!found) continue;
                Console.WriteLine("[Transaction] Transaction with ID [{0}] complete", transactionId);
                break;
            }
        }
        if (!found)
        {
            Console.WriteLine("[Transaction] Error: TransactionSuccess timeout");
            return;
        }
        else
        {
            Console.WriteLine("[Transaction] Received Transaction Success [{0}]", transactionId);
        }

    }

    //Helper function
    private void GetNetworkNames()
    {
        var stringpath = Directory.GetCurrentDirectory();
        var nodespath = Path.Combine(stringpath, "Nodes");
        var di = new DirectoryInfo(nodespath);
        List<FileInfo> files = new();

        files.AddRange(new DirectoryInfo(nodespath).GetFiles("*.csv").Where(p =>
            p.Extension.Equals(".csv", StringComparison.CurrentCultureIgnoreCase))
            .ToArray());

        files.ForEach(file => KnownNetworks.Add(file.Name.Split(".")[0]));
    }
    private static Cluster? GetCluster(string network)
    {
        var endpoints = GetEndpoints(network);
        if (endpoints == null)
            return null;

        //Make an cluster of the csv file
        Cluster TransactionNodes = new();

        foreach (string endpoint in endpoints.Skip(1))
        {
            //Console.WriteLine(endpoint);
            string[] endpointPropterties = endpoint.Split(',');
            int e_id = Int32.Parse(endpointPropterties[0]);
            System.Net.IPAddress e_ip = System.Net.IPAddress.Parse(endpointPropterties[1]);
            int e_port = Int32.Parse(endpointPropterties[2]);
            Node e_node = new Node(e_id, e_ip, e_port);

            TransactionNodes.TryAdd(e_id, e_node);
        }

        return TransactionNodes;
    }

    private static string[]? GetEndpoints(string network)
    {
        string NETWORK_FILE_PATH = "Nodes/" + network + ".csv";
        if (!File.Exists(NETWORK_FILE_PATH))
        {
            Console.WriteLine("[Transaction] Error: Network " + NETWORK_FILE_PATH + " not found");
            return null;
        }

        string[] endpoints;

        try
        {
            endpoints = File.ReadAllLines(NETWORK_FILE_PATH);
        }
        catch (Exception ex) { Console.WriteLine(ex.Message); return null; }

        if (endpoints.Length == 0)
        {
            Console.WriteLine("[Transaction] Error: Empty network file: " + NETWORK_FILE_PATH);
            return null;
        }

        return endpoints;
    }

    //Receiving messages

    public void OnTransactionProposal(TransactionProposal transactionProposal)
    {
        if(_parentNode.isPresident)
            TransactionProposals.Enqueue((transactionProposal._networkName, transactionProposal._decree, transactionProposal._saveToLedger));
    }
    public async Task OnReceiveFindLeader(FindLeader findLeaderMsg, Node node)
    {
        if (_parentNode.PresidentNode == null || findLeaderMsg._networkName != _parentNode.NetworkName) return;
        Leader leader = new Leader(_parentNode.Client._messageIdCounter,
                                    _parentNode.Id,
                                    _parentNode.NetworkName,
                                    _parentNode.PresidentNode.Id,
                                    _parentNode.PresidentNode.EndPoint.ToString());
        await _parentNode.Client.SendMessageToNode(leader, node, false, false);
        Console.WriteLine("[Transaction] Received FindLeader message from [{0},{1}]", node.Id, node.EndPoint.ToString());
    }
    public async Task OnLeader(Leader leader)
    {
        Console.WriteLine(leader._ip);
        System.Net.IPEndPoint ipe;
        try
        {
            ipe = System.Net.IPEndPoint.Parse(leader._ip);
        }
        catch (Exception ex) { Console.WriteLine("[Transaction] received invalid IP"); return; }
        Node leaderNode = new Node(ipe);

        //put this node as leader of given network
        NetworkLeaderNodes[leader._networkName] = leaderNode;
        Console.WriteLine("[Transaction] added node [{0}] as leader of network [{1}]", leader._nodeId, leader._networkName);
    }

    public async Task OnTransactionSuccess(TransactionSuccess message)
    {
        if(FoundTransactionIds.TryAdd(message._transactionId, true))
            Console.WriteLine("[Transaction] Received TransactionSuccess message with id[{0}]", message._transactionId);
    }

    public async Task OnTransaction(Transaction transaction, Node node)
    {
        if (transaction._networkName == _parentNode.NetworkName)
        {
            //add message information to the transactionqueue
            TransactionQueue.Enqueue((transaction._networkName, transaction._transactionId, transaction._decree, node));
            Console.WriteLine("[Transaction] Received transaction message from [{0}, id={1}][transactionID={2}][decree={3}]", transaction._networkName, transaction._transactionId, transaction._decree);
        }
    }
}
