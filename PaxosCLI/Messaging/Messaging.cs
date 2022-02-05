using PaxosCLI.NodeAgents;
using System.Globalization;

namespace PaxosCLI.Messaging;

/// <summary>
/// Contains generic information which every kind of concrete message class should contain.
/// </summary>
public class Message
{
    public decimal _id { get; protected set; }
    public int _senderId { get; protected set; }
    public bool _doResend { get; protected set; }

    public Message() { }

    public virtual byte[] ToByteArray()
    {
        return new byte[] { };
    }
}

/// <summary>
/// A message to confirm the arrival of a data message.
/// </summary>
public class ArrivalConfirmationMessage : Message
{
    public decimal _replyToMessageId;

    public ArrivalConfirmationMessage(long messageIdCounter, decimal replyToMessageId, int senderId)
    {
        _doResend = false;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _replyToMessageId = replyToMessageId;
        _senderId = senderId;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},ACM;{1}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _replyToMessageId));
    }
}

/// <summary>
/// A message sent by a node to each peer, to check the online status.
/// </summary>
public class Heartbeat : Message
{
    public int _isPresident;
    public decimal _lastTried;

    //for sending
    public Heartbeat(Node node, long messageIdCounter)
    {
        _doResend = false;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, node.Id);
        _lastTried = node.lastTried;
        _isPresident = Convert.ToInt32(node.isPresident);
    }

    //for receiving/parsing
    public Heartbeat(long messageIdCounter, int senderId, decimal lastTried, int isPresident)
    {
        _doResend = false;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;
        _lastTried = lastTried;
        _isPresident = isPresident;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(this.ToString());
    }
    public override string ToString()
    {
        return String.Format("{0},HB;{1},{2}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _lastTried.ToString(CultureInfo.InvariantCulture),
                                                                _isPresident
                                                                );
    }
}


public class NextBallot : Message
{
    public decimal _ballotId;
    public long _hasDecreesUntil;

    public NextBallot(long messageIdCounter, int senderId, decimal ballotId, long hasDecreesUntil)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _ballotId = ballotId;
        _hasDecreesUntil = hasDecreesUntil;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},NB;{1},{2}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _ballotId.ToString(CultureInfo.InvariantCulture),
                                                                _hasDecreesUntil));
    }
}


public class LastVote : Message
{
    public decimal _nextBal, _prevBal;
    public byte[] _prevDecree;
    public string _missingDecrees;

    public LastVote(long messageIdCounter, decimal nextBal, int senderId, decimal prevBal, byte[] prevDecree, string missingDecrees)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _nextBal = nextBal;
        _prevBal = prevBal;
        _prevDecree = prevDecree;
        _missingDecrees = missingDecrees;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},LV;{1},{2},{3},{4}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _nextBal.ToString(CultureInfo.InvariantCulture),
                                                                _prevBal.ToString(CultureInfo.InvariantCulture),
                                                                MessageHelper.ByteArrayToString(_prevDecree),
                                                                _missingDecrees));
    }

    public override string ToString()
    {
        return String.Format("\nNextBal={0}\nNode={1}\nPrevBal={2}\nPrevDecree={3}\nMissingDecreeString={4}",
                                _nextBal.ToString(CultureInfo.InvariantCulture),
                                _senderId,
                                _prevBal.ToString(CultureInfo.InvariantCulture),
                                MessageHelper.ByteArrayToString(_prevDecree),
                                _missingDecrees);
    }
}

public class BeginBallot : Message
{
    public decimal _ballotId;
    public byte[] _decree;

    public BeginBallot(long messageIdCounter, int senderId, decimal ballotId, byte[] decree)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _ballotId = ballotId;
        _decree = decree;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(this.ToString());
    }
    public override string ToString()
    {
        return String.Format("{0},BB;{1},{2}",
                                                        _id.ToString(CultureInfo.InvariantCulture),
                                                        _ballotId.ToString(CultureInfo.InvariantCulture),
                                                        MessageHelper.ByteArrayToString(_decree));
    }
}

public class Voted : Message
{
    public decimal _ballotId;

    public Voted(long messageIdCounter, int senderId, decimal ballotId)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _ballotId = ballotId;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},VD;{1}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _ballotId.ToString(CultureInfo.InvariantCulture)));
    }
}

public class Success : Message
{
    public decimal _ballotId;
    public byte[] _decree;
    public long _decreeId;

    public Success(long messageIdCounter, int senderId, byte[] decree, long decreeId)
    {
        _doResend = true;
        _senderId = senderId;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);

        _decree = decree;
        _decreeId = decreeId;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(this.ToString());
    }
    public override string ToString()
    {
        return String.Format("{0},SS,{1};{2}",
                                    _id.ToString(CultureInfo.InvariantCulture),
                                    _decreeId,
                                    MessageHelper.ByteArrayToString(_decree));
    }
}
/// <summary>
/// Add Success and BeginBallot in one message. New signature is "SBB"
/// </summary>
public class SuccessBeginBallot : Message
{
    public BeginBallot beginBallotMsg;
    public Success successMsg;

    public SuccessBeginBallot(Success success, BeginBallot beginBallot)
    {
        _id = beginBallot._id;
        beginBallotMsg = beginBallot;
        successMsg = success;
    }
    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(this.ToString());
    }
    public override string ToString()
    {
        return String.Format("{0},SBB;{1};{2}",
                                    _id.ToString(CultureInfo.InvariantCulture),
                                    successMsg.ToString(),
                                    beginBallotMsg.ToString());
}
}

public class UpdateBallotNumber : Message
{
    public decimal _nextBal;

    public UpdateBallotNumber(long messageIdCounter, int senderId, decimal nextBal)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _nextBal = nextBal;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},UBN;{1}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _nextBal.ToString(CultureInfo.InvariantCulture)));
    }
}

public class DecreeProposal : Message
{
    public byte[] _decree { get; private set; }

    public DecreeProposal(long messageIdCounter, int senderId, byte[] decree)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _decree = decree;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},DP;{1}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                MessageHelper.ByteArrayToString(_decree)));
    }
}

public class RequestMissingEntriesMessage : Message
{
    public long _decreeId { get; private set; }
    public string _entriesInOwnLedgerString;

    public RequestMissingEntriesMessage(long messageIdCounter, int senderId, long decreeId, string entriesInOwnLedgerString)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _decreeId = decreeId;
        _entriesInOwnLedgerString = entriesInOwnLedgerString;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},RME;{1},{2}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _decreeId,
                                                                _entriesInOwnLedgerString));
    }
}

public class InformMissingEntriesMessage : Message
{
    public string _entriesString { get; private set; }

    public InformMissingEntriesMessage(long messageIdCounter, int senderId, string entriesString)
    {
        _doResend = true;
        _id = MessageHelper.CreateUniqueMessageId(messageIdCounter, senderId);
        _senderId = senderId;

        _entriesString = entriesString;
    }

    public override byte[] ToByteArray()
    {
        return MessageHelper.StringToByteArray(String.Format("{0},IME;{1}",
                                                                _id.ToString(CultureInfo.InvariantCulture),
                                                                _entriesString));
    }
}



