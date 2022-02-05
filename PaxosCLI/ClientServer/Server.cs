using PaxosCLI.Database;
using PaxosCLI.Messaging;
using PaxosCLI.NodeAgents;
using System.Net;
using System.Net.Sockets;

namespace PaxosCLI.ClientServer;

/// <summary>
/// A server is the 'receiver' part of the node.
/// It continuously listens for any new messages it can receive, and handles this type of message accordingly.
/// If a message has to be sent back, it will call a method in the client of the node the server belongs to.
/// </summary>
public class Server
{
    public IPEndPoint _IPEndPoint { get; private set; }
    private UdpClient listener;
    public bool isListening { get; private set; }
    private Node _parentNode;
    private Queue<decimal> recentlyReceivedMessages;
    private static readonly int MAX_LOG_ENTRY_SIZE = 1000;

    //online check
    private Thread checkClusterStatusThread;
    private Thread heartBeatThread;
    private static readonly int PRESIDENT_AFTER_MILLISECONDS = 1000; //value T of page 14 part-time parliament
    private static readonly int HEARTBEAT_INTERVAL = PRESIDENT_AFTER_MILLISECONDS - (11 * Node.MINUTE_IN_PAXOS_TIME);
    private static readonly int CLUSTER_ONLINE_STATUS_CHECK_INTERVAL = PRESIDENT_AFTER_MILLISECONDS;
    private static readonly int OFFLINE_AFTER_MILLISECONDS = PRESIDENT_AFTER_MILLISECONDS;

    /// <summary>
    /// Creates a new server for this node.
    /// </summary>
    /// <param name="node">A reference back to the node this server belongs to</param>
    public Server(Node node)
    {
        _IPEndPoint = node.EndPoint;
        _parentNode = node;
        recentlyReceivedMessages = new Queue<decimal>();
        listener = _parentNode.Socket;

        StartListening();
        InitHeartbeatSendingThread();
        InitCheckClusterStatusThread();
        InitCheckPresidentThread();
    }

    /// <summary>
    /// If a message has been received, this code checks:
    /// (1) if the message has been received before.
    /// (2) The type of message
    /// Based on this information, appropiate behaviour is chosen.
    /// </summary>
    /// <param name="request">The received request</param>
    private async Task ReceiveRequest(byte[] request, IPEndPoint ip) 
    {
        Message receivedMessage = MessageHelper.ByteArrayToMessage(request);
        Node sender = _parentNode.Peers.GetNodeByIp(ip);
        
        if (!(sender.IPAddress.Equals(IPAddress.Parse("127.0.0.1")) && sender.Id.Equals(Int32.MinValue))) { //check if sender is a known Node

            UpdateOnlineStatus(sender);

            if (receivedMessage._doResend && !receivedMessage.GetType().Name.Equals("ArrivalConfirmationMessage"))
            {
                await _parentNode.Client.SendArrivalConfirmation(receivedMessage);
                bool receivedMsgBefore = ReceivedMessageBefore(receivedMessage);
                if (receivedMsgBefore) return; //if received the message before: do nothing
            }

            switch (receivedMessage.GetType().Name)
            {
                case "ArrivalConfirmationMessage":
                    ArrivalConfirmationMessage arrivalConfirmationMessage = (ArrivalConfirmationMessage)receivedMessage;
                    _parentNode.Client.ConfirmArrival(arrivalConfirmationMessage);
                    break;
                case "Heartbeat":
                    Heartbeat hb = (Heartbeat)receivedMessage;
                    Node receivedFromNode = _parentNode.Peers.GetNodeById(hb._senderId);
                    await OnHeartbeat(hb);
                    break;
                case "NextBallot":
                    NextBallot nextBallotMsg = (NextBallot)receivedMessage;
                    await _parentNode.Acceptor.OnReceiveNextBallot(nextBallotMsg);
                    break;
                case "LastVote":
                    LastVote lastVote = (LastVote)receivedMessage;
                    _parentNode.Proposer.ReceiveLastVoteMessage(lastVote);
                    break;
                case "SuccessBeginBallot":
                    SuccessBeginBallot successBeginBallotMsg = (SuccessBeginBallot)receivedMessage;
                    await _parentNode.Acceptor.OnReceiveSuccessBeginBallot(successBeginBallotMsg);
                    break;
                case "BeginBallot":
                    BeginBallot beginBallot = (BeginBallot)receivedMessage;
                    await _parentNode.Acceptor.OnReceiveBeginBallot(beginBallot);
                    break;
                case "Voted":
                    Voted voted = (Voted)receivedMessage;
                    _parentNode.Proposer.ReceiveVotedMessage(voted);
                    break;
                case "Success":
                    Success success = (Success)receivedMessage;
                    await _parentNode.Learner.ReceiveSuccess(success);
                    break;
                case "UpdateBallotNumber":
                    UpdateBallotNumber newBallot = (UpdateBallotNumber)receivedMessage;
                    await _parentNode.Proposer.ReceiveNewerBallotNumber(newBallot._nextBal);
                    break;
                case "DecreeProposal":
                    DecreeProposal decreeProposal = (DecreeProposal)receivedMessage;
                    _parentNode.Proposer.OnDecreeProposal(decreeProposal);
                    break;
                case "RequestMissingEntriesMessage":
                    RequestMissingEntriesMessage missingEntriesMessage = (RequestMissingEntriesMessage)receivedMessage;
                    await _parentNode.Proposer.InformMissingDecrees(missingEntriesMessage);
                    break;
                case "InformMissingEntriesMessage":
                    InformMissingEntriesMessage informAboutDecree = (InformMissingEntriesMessage)receivedMessage;
                    await _parentNode.Learner.WriteMissingDecreesToLedger(informAboutDecree._entriesString);
                    break;

                //TransactionMessages
                case "TransactionProposal": //Notice this can only be received if the sender is in current network
                    TransactionProposal transactionProposal = (TransactionProposal)receivedMessage;
                    _parentNode.NetworkTransaction.OnTransactionProposal(transactionProposal);
                    break;
                case "FindLeader":
                    FindLeader findLeader = (FindLeader)receivedMessage;
                    await _parentNode.NetworkTransaction.OnReceiveFindLeader(findLeader, sender);
                    break;
                case "Leader":
                    Leader leader = (Leader)receivedMessage;
                    await _parentNode.NetworkTransaction.OnLeader(leader);
                    break;
                case "Transaction":
                    Transaction transaction = (Transaction)receivedMessage;
                    await _parentNode.NetworkTransaction.OnTransaction(transaction, sender);
                    break;
                case "TransactionSuccess":
                    TransactionSuccess transactionSuccess = (TransactionSuccess)receivedMessage;            
                    await _parentNode.NetworkTransaction.OnTransactionSuccess(transactionSuccess);
                    break;
                default:
                    Console.WriteLine("Unknown request received.");
                    break;
            }
        }
        else //If sender is not known in the network, it can only be other network-nodes trying to send a transaction
        {
            sender = new(ip);
            switch (receivedMessage.GetType().Name)
            {
                //TransactionMessages
                case "FindLeader":
                    FindLeader findLeader = (FindLeader)receivedMessage;
                    await _parentNode.NetworkTransaction.OnReceiveFindLeader(findLeader, sender);
                    break;
                case "Leader":
                    Leader leader = (Leader)receivedMessage;
                    await _parentNode.NetworkTransaction.OnLeader(leader);
                    break;
                case "Transaction":
                    Transaction transaction = (Transaction)receivedMessage;
                    await _parentNode.NetworkTransaction.OnTransaction(transaction, sender);
                    break;
                case "TransactionSuccess":
                    TransactionSuccess transactionSuccess = (TransactionSuccess)receivedMessage;            
                    await _parentNode.NetworkTransaction.OnTransactionSuccess(transactionSuccess);
                    break;
                default:
                    Console.WriteLine("Unknown request received." + MessageHelper.ByteArrayToString(request));
                    break;
            }
        }
    }


    /// <summary>
    /// Checks if a message has been received before. Received messages need not to be acted upon again.
    /// </summary>
    /// <param name="message">Message to check if received before</param>
    /// <returns>If in log of recent messages, true. If not, false.</returns>
    private bool ReceivedMessageBefore(Message message)
    {
        if (recentlyReceivedMessages.Contains(message._id))
        {
            return true;
        }
        else
        {
            //add data message to the log of recently received message
            recentlyReceivedMessages.Enqueue(message._id);

            if (recentlyReceivedMessages.Count() == MAX_LOG_ENTRY_SIZE)
            {
                recentlyReceivedMessages.Dequeue();
            }
            return false;
        }
    }

    private void StartListening()
    {
        Console.WriteLine("[Server] Initialising...");
        DateTime listeningSince = DateTime.Now;
        while ((DateTime.Now - listeningSince).TotalMilliseconds <= PRESIDENT_AFTER_MILLISECONDS)
        {
            //Wait for potential new president
        }
        Console.WriteLine(_IPEndPoint);
        //listener.Client.Bind(_IPEndPoint);

        //added for 'An existing connection was forcibly closed by the remote host'-error. 
        if (OperatingSystem.IsWindows())
        {
            uint IOC_IN = 0x80000000;
            uint IOC_VENDOR = 0x18000000;
            uint SIO_UDP_CONNRESET = IOC_IN | IOC_VENDOR | 12;
            listener.Client.IOControl((int)SIO_UDP_CONNRESET, new byte[] { Convert.ToByte(false) }, null);
        }

        listener.BeginReceive(new AsyncCallback(ReceiveMessageAsync), null);
        isListening = true;
        Console.WriteLine("[Server] Listening.");
    }

    /// <summary>
    /// This code allows messages to be recieved in an async way
    /// </summary>
    private void ReceiveMessageAsync(IAsyncResult result)
    {
        IPEndPoint remoteIpEndPoint = new IPEndPoint(IPAddress.Any, 0);
        byte[] request = listener.EndReceive(result, ref remoteIpEndPoint);
        listener.BeginReceive(new AsyncCallback(ReceiveMessageAsync), null);
        Task.Factory.StartNew(async () => await ReceiveRequest(request, remoteIpEndPoint));
    }

    /// <summary>
    /// Updates the moment it last received a message from a node.
    /// </summary>
    /// <param name="sender">The peer which sent a message to this node.</param>
    private void UpdateOnlineStatus(Node sender)
    {
        sender.IsOnline = true;
        DateTime currentTime = DateTime.Now;
        sender.LastMessageReceivedAt = currentTime;
    }

    /// <summary>
    ///   Starts the thread which sends out pings to peers, and checks the online status of other nodes.
    /// </summary>
    private void InitCheckClusterStatusThread()
    {
        checkClusterStatusThread = new Thread(() =>
                {
                    while (isListening && _parentNode.Client.IsSending)
                    {
                        UpdateOnlineStatus(_parentNode.Peers);
                        checkClusterStatusThread.Join(CLUSTER_ONLINE_STATUS_CHECK_INTERVAL);
                    }
                });
        checkClusterStatusThread.Start();
    }


    /// <summary>
    /// Starts the thread which periodically sends heartbeats to all peers.
    /// </summary>
    private void InitHeartbeatSendingThread()
    {
        heartBeatThread = new Thread(async () =>
                {
                    while (isListening && _parentNode.Client.IsSending)
                    {
                        await _parentNode.Client.SendHeartbeatToCluster(_parentNode.Peers);
                        heartBeatThread.Join(HEARTBEAT_INTERVAL);
                    }
                });
        heartBeatThread.Start();
    }


    /// <summary>
    /// Periodically checks if this node has become the president
    /// </summary>
    private void InitCheckPresidentThread()
    {
        Task.Factory.StartNew(async () =>
                {
                    while (isListening && _parentNode.Client.IsSending)
                    {
                        await Task.Delay(PRESIDENT_AFTER_MILLISECONDS);
                        await CheckIfPresident(_parentNode.Peers);
                    }
                });
    }

    /// <summary>
    /// Checks for each node in a cluster when this node last heard from a peer, and determines if the peer is online or offline.
    /// </summary>
    /// <param name="peers">The cluster with all nodes to check for online status.</param>
    private void UpdateOnlineStatus(Cluster peers)
    {
        DateTime checkTime = DateTime.Now;

        foreach (Node peer in peers.Values)
        {
            if ((checkTime - peer.LastMessageReceivedAt).TotalMilliseconds > OFFLINE_AFTER_MILLISECONDS)
                peer.IsOnline = false;
            else
                peer.IsOnline = true;
        }
        _parentNode.UpdateOnlineNodes();
    }

    /// <summary>
    ///   A node is a president after it hasn't received a message from a higher numbered node after
    /// </summary>
    private async Task CheckIfPresident(Cluster peers)
    {
        DateTime checkTime = DateTime.Now;

        foreach (Node peer in peers.Values)
        {
            if ((checkTime - peer.LastMessageReceivedAt).TotalMilliseconds <= PRESIDENT_AFTER_MILLISECONDS
                && peer.Id < _parentNode.Id)
            {
                //not a president
                if (_parentNode.isPresident)
                {
                    Console.WriteLine("[Server] Not a president anymore.");
                }
                _parentNode.isPresident = false;
                _parentNode.Proposer.presidentInitTaskFinished = false;
                return;
            }
        }

        if (!_parentNode.isPresident)
        {
            Console.WriteLine("[Server] Became president.");
            _parentNode.isPresident = true;
            await _parentNode.Proposer.OnBecomingPresident();
        }
    }

    /// <summary>
    /// Updates information of the other node if it has received a heartbeat message.
    /// </summary>
    /// <param name="hb">The heartbeat message</param>
    private async Task OnHeartbeat(Heartbeat hb)
    {
        Node receivedFromNode = _parentNode.Peers.GetNodeById(hb._senderId);
        receivedFromNode.isPresident = hb._isPresident == 1 ? true : false;

        if (receivedFromNode.Id < _parentNode.Id || receivedFromNode.isPresident)
        {
            _parentNode.isPresident = false;
        }

        if (receivedFromNode.isPresident)
        {
            _parentNode.PresidentNode = receivedFromNode;
        }

        if (Math.Truncate(hb._lastTried) > Math.Truncate(_parentNode.lastTried))
        {
            _parentNode.lastTried = MessageHelper.CreateUniqueMessageId((long)Math.Truncate(hb._lastTried), _parentNode.Id);
            await _parentNode.LedgerHelper.SavePaxosProgressAsync(_parentNode);
        }
    }
}

