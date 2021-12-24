using System.Globalization;

namespace PaxosCLI.Messaging;


/// <summary>
/// DecreeProposal + network_node
/// </summary>
 public class TransactionProposal : Message
{
    public string _networkName;
    public byte[] _decree;

    public TransactionProposal(long messageIdCounter, int senderId, string networkName, byte[] decree)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _decree = decree;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},TP,{1};{2}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _decree));
    }
}

public class FindLeader : Message
{
    public string _networkName;

    public FindLeader(long messageIdCounter, int senderId, string networkName)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},FL,{1};",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName));
    }
}

public class Leader : Message
{
    public string _networkName;
    public int _nodeId;
    public string _ip;

    public Leader(long messageIdCounter, int senderId, string networkName, int nodeId, string ip)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _nodeId = nodeId;
        _ip = ip;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},L,{1};{2},{3}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _nodeId,
                                                                _ip));
    }
}

public class Transaction : Message
{
    public int _transactionId;
    public string _networkName;
    public byte[] _decree;

    public Transaction(long messageIdCounter, int senderId, string networkName, int transactionId, byte[] decree)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _transactionId = transactionId;
        _decree = decree;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},T,{1},{2};{3}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _transactionId,
                                                                _decree));
    }

}

public class TransactionSuccess : Message
{
    public int _transactionId;
    public string _networkName;

    public TransactionSuccess(long messageIdCounter, int senderId, string networkName, int transactionId)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _transactionId = transactionId;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},TS,{1},{2};",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _transactionId));
    }
} 


/* PART of OLD PROTOCOL
public class BeginTransaction : Message
{
    public string _network_name;
    public int _transactionID;
    public decimal _decreeID;
    public int[] _sendToIds;

    public BeginTransaction(long messageIdCounter, int senderId, string network_name, int transactionID, decimal decreeID, int[] sendToIds)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;
        _network_name = network_name;
        _transactionID = transactionID;
        _decreeID = decreeID;
        _sendToIds = sendToIds;
    }
    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},BT,{1},{2},{3};{4}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _network_name,
                                                                _transactionID.ToString(),
                                                                _decreeID.ToString(),
                                                                string.Join(",", _sendToIds)));
    }
}
/// <summary>
/// Transaction Message, Send this to a node of the other network
/// </summary>
public class Transaction : Message
{
    public string _networkName;
    public byte[] _decree;
    public int _transactionId;

    public Transaction(long messageIdCounter, int senderId, string networkName, byte[] decree, int transaction_id)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _decree = decree;
        _transactionId = transaction_id;
    }
    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},TS,{1},{2};{3}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _transactionId,
                                                                MessageHelper.ByteArrayToString(_decree)));
    }
}

class TransactionConfirmation : Message
{
    public byte[] _decree;
    public int _transactionId;
    public string _networkName;
    public decimal _ballotId;

    public TransactionConfirmation(long messageIdCounter, int senderId, string networkName, byte[] decree, int transactionId, decimal ballotId)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _decree = decree;
        _transactionId = transactionId;
        _ballotId = ballotId;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},TC,{1},{2},{3};{4}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _transactionId,
                                                                _ballotId,
                                                                MessageHelper.ByteArrayToString(_decree)));
    }
}

*/