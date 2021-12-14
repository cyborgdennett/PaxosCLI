using System;
using System.Text;
using PaxosCLI.NodeAgents;
using System.Globalization;

namespace PaxosCLI.Messaging;

/// <summary>
/// Contains code for the preparing and decoding of messages sent between nodes.
/// Messages are made as a Message concrete class, then encoded to a byte array for sending.
/// When receiving such as message, a message is decoded to a concrete Message class.
/// </summary>
public static class MessageHelper
{

    /// <summary>
    /// Fulfills requirement B1 of The Part-Time Parliament.
    /// Creates a messageId by glueing a message id to a node id.
    /// </summary>
    /// <param name="messageId">The message id to be sent by node</param>
    /// <param name="nodeId">The node id</param>
    /// <returns>A unique message id, e.g. (3.1.) where 3=messageId and 1=nodeId</returns>
    public static decimal CreateUniqueMessageId(long messageId, int nodeId)
    {
        return Convert.ToDecimal(String.Format("{0}.{1}", messageId, nodeId), CultureInfo.InvariantCulture);
    }

    public static String ByteArrayToString(byte[] bytesToParse)
    {
        return Encoding.ASCII.GetString(bytesToParse);
    }

    public static byte[] StringToByteArray(string str)
    {
        return Encoding.ASCII.GetBytes(str);
    }

    /// <summary>
    ///  Decodes a byte array to a message, if possible.
    ///  A typical message (e.g. a heartbeat) looks like: 1.1,HB;2.1,1 
    ///  where data before ';' is generic message information: message Id and message Type
    ///  and data after ';' is data specific for the type of message
    /// </summary>
    /// <param name="bytes">The bytes to parse to a message.</param>
    /// <returns>A message</returns>
    public static Message ByteArrayToMessage(byte[] bytes)
    {
        try
        {
            // Console.WriteLine("[{0}]", ByteArrayToString(bytes));
            // Console.WriteLine("[Amount of bytes: {0}]", bytes.Length);

            //separate all the information of the message
            string[] splitMessage = ByteArrayToString(bytes).Split(';');

            //generic information for every kind of message
            string[] messageInformation = splitMessage[0].Split(',');
            string[] fullMessageSplit = messageInformation[0].Split('.');
            long messageId = long.Parse(fullMessageSplit[0]);
            int senderId = Int32.Parse(fullMessageSplit[1]);
            string messageType = messageInformation[1];

            //the content specialised for the type of message
            string[] messageContent = splitMessage[1].Split(',');

            switch (messageType)
            {
                case "ACM":
                    {
                        decimal replyToMessageId = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        return new ArrivalConfirmationMessage(messageId, replyToMessageId, senderId);
                    }
                case "HB":
                    {
                        decimal lastTried = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        int isPredident = Int32.Parse(messageContent[1]);
                        return new Heartbeat(messageId, senderId, lastTried, isPredident);
                    }
                case "NB":
                    {
                        decimal ballotId = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        long hasDecreesUntil = long.Parse(messageContent[1]);
                        return new NextBallot(messageId, senderId, ballotId, hasDecreesUntil);
                    }
                case "LV":
                    {
                        decimal nextBalId = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        decimal previousBallotId = Decimal.Parse(messageContent[1], CultureInfo.InvariantCulture);
                        byte[] previousDecree = StringToByteArray(messageContent[2]);
                        string missingDecrees = messageContent[3];
                        return new LastVote(messageId, nextBalId, senderId, previousBallotId, previousDecree, missingDecrees);
                    }
                case "BB":
                    {
                        decimal ballotId = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        byte[] decree = StringToByteArray(messageContent[1]);
                        return new BeginBallot(messageId, senderId, ballotId, decree);
                    }
                case "VD":
                    {
                        decimal ballotId = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        return new Voted(messageId, senderId, ballotId);
                    }
                case "SS":
                    {
                        byte[] decree = StringToByteArray(messageContent[0]);
                        long decreeId = long.Parse(messageContent[1]);
                        return new Success(messageId, senderId, decree, decreeId);
                    }
                case "UBN":
                    {
                        decimal nextBal = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        return new UpdateBallotNumber(messageId, senderId, nextBal);
                    }
                case "DP":
                    {
                        byte[] decree = StringToByteArray(messageContent[0]);
                        return new DecreeProposal(messageId, senderId, decree);
                    }
                case "RME":
                    {
                        long decreeId = long.Parse(messageContent[0]);
                        string hasDecreesWritten = messageContent[1];
                        return new RequestMissingEntriesMessage(messageId, senderId, decreeId, hasDecreesWritten);
                    }
                case "IME":
                    {
                        string missingEntries = messageContent[0];
                        return new InformMissingEntriesMessage(messageId, senderId, missingEntries);
                    }
                case "SBB":
                    {

                        //the content specialised for the type of message
                        string[] messageContentBB = splitMessage[2].Split(',');

                        //gets the parts of Success
                        byte[] decreeSS = StringToByteArray(messageContent[0]);
                        long decreeIdSS = long.Parse(messageContent[1]);

                        //gets the parts of the BeginBallot
                        decimal ballotIdBB = Decimal.Parse(messageContent[0], CultureInfo.InvariantCulture);
                        byte[] decreeBB = StringToByteArray(messageContent[1]);

                        return new SuccessBeginBallot(messageId, senderId,
                                                      decreeSS, decreeIdSS,
                                                      ballotIdBB, decreeBB);
                    }
                default:
                    {
                        Console.WriteLine("Unknown message type.");
                        return new Message();
                    }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("Failed parsing message.");
            Console.WriteLine(e.StackTrace);
            return new Message();
        }
    }

}