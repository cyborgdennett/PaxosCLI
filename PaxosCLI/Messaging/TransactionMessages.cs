using System.Globalization;

namespace PaxosCLI.Messaging;


/// <summary>
/// DecreeProposal + network_node
/// </summary>
public class TransactionProposal : Message
{
    public string _networkName;
    public byte[] _decree;
    public bool _saveToLedger;

    public TransactionProposal(long messageIdCounter, int senderId, string networkName, byte[] decree, bool saveToLedger = false)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _networkName = networkName;
        _decree = decree;
        _saveToLedger = saveToLedger;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},TP,{1},{2};{3}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _networkName,
                                                                _saveToLedger ? 1 : 0,
                                                                MessageHelper.ByteArrayToString(_decree)));
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
    public string _ip;//ip+port

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
        return MessageHelper.StringToByteArray(String.Format("{0},L,{1},{2};{3}",
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
                                                                MessageHelper.ByteArrayToString(_decree)));
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
