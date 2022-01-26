using Microsoft.VisualStudio.TestTools.UnitTesting;

using PaxosCLI.NodeAgents;
using PaxosCLI;
using System;
using System.IO;
using System.Text;
using System.Net;
using System.Net.NetworkInformation;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace PaxosCLITest
{
    [TestClass]
    public class StartupTests
    {

        [TestInitialize()]
        public void startup()
        {
            try
            {
                //Cleanup all node ip files
                var stringpath = Directory.GetCurrentDirectory();
                var nodespath = Path.Combine(stringpath, "Nodes");
                if (Directory.Exists(nodespath))
                {
                    Directory.Delete(nodespath, true);
                }
                Directory.CreateDirectory(nodespath);
                Thread.Sleep(5);

                //Cleanup ledgers
                var di = new DirectoryInfo(stringpath);
                List<FileInfo> files = new();
                foreach (string ext in new List<string>() { ".db", ".db-wal", ".db-shm" })
                {
                    files.AddRange(new DirectoryInfo(stringpath).GetFiles("*" + ext).Where(p =>
                      p.Extension.Equals(ext, StringComparison.CurrentCultureIgnoreCase))
                      .ToArray());
                }
                foreach(FileInfo file in files)
                {
                    //Console.WriteLine(file.FullName);
                    File.Delete(file.FullName);
                }

            }
            catch (Exception ex) { }
        }
        [TestCleanup()]
        public void cleanup()
        {

        }
        public void addNodeFile(string network, List<(int, int)> idport)
        {
            var stringpath = Directory.GetCurrentDirectory();
            var nodespath = Path.Combine(stringpath, "Nodes");
            var filepath = Path.Combine(nodespath, network + ".csv");
            if (!System.IO.File.Exists(filepath))
            {
                File.Create(filepath).Dispose();
            }
            Thread.Sleep(5);
            using (System.IO.FileStream fs = File.OpenWrite(filepath))
            {
                var ip = GetLocalActiveIpAddress();
                StringBuilder ipstring = new StringBuilder();
                foreach((int id, int port) in idport)
                {
                    ipstring.Append("\n" + id.ToString() + "," + ip.ToString() + "," + port.ToString());
                }
                byte[] bytestring = Encoding.UTF8.GetBytes(ipstring.ToString());
                fs.Write(bytestring, 0, bytestring.Length);
            }
        }
        class TestNode
        {
            public volatile Node Node;
            public Thread Thread;
            int port { get; set; }
            string networkName { get; set; }
            public void start()
            {
                Thread = new Thread(async() =>
                {
                    try
                    {
                        Node = new Node(networkName, port);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                });
                Thread.Start();
            }
            public TestNode(int port = 10000, string network = "")
            {
                this.port = port;
                this.networkName = network;
            }
        }

        public IPAddress GetLocalActiveIpAddress()
        {
            foreach (var networkInterface in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (networkInterface.NetworkInterfaceType != NetworkInterfaceType.Wireless80211 &&
                    (networkInterface.NetworkInterfaceType != NetworkInterfaceType.Ethernet || networkInterface.OperationalStatus != OperationalStatus.Up))
                {
                    continue;
                }

                foreach (var uniIpAddrInfo in networkInterface.GetIPProperties().UnicastAddresses.Where(x => networkInterface.GetIPProperties().GatewayAddresses.Count > 0))
                {
                    if (uniIpAddrInfo.Address.AddressFamily == AddressFamily.InterNetwork)
                        return IPAddress.Parse(uniIpAddrInfo.Address.ToString());
                }
            }
            Console.WriteLine("Local IPv4 couldn't be found in list of authorized nodes.");
            return null;
        }


        [TestMethod]
        public void TestOneDecreeProgram()
        {
            addNodeFile("testnetwork", new List<(int, int)> { (1, 10000), (2, 10001) });
            addNodeFile("HomeNetwork-0123", new List<(int, int)> { (1, 10002), (2, 10003) });

            var testNodeList = new List<TestNode>() { new TestNode(10000, "testnetwork"),
                                                        new TestNode(10001, "testnetwork"),
                                                        new TestNode(10002, "HomeNetwork-0123"),
                                                        new TestNode(10003, "HomeNetwork-0123")};
            testNodeList.ForEach(node => node.start());


            for (int i = 0; i < 8; i++) {
                Console.WriteLine("[Test] Initialising... {0}", i);
                Thread.Sleep(500);
            }
            Console.WriteLine("[Test] Wakeup...");

            if (testNodeList.Exists(x => x.Node.PortNumber == 10001))
                testNodeList[1].Node.testinput("", "This is a test 1234 beep boop");
            Thread.Sleep(2000);

            //Assert.IsTrue(true);
            //Volatile List<LedgerEntry> ;
            Thread t = new Thread(async () =>
            {
                var entries2 = await testNodeList[1].Node.LedgerHelper.GetEntries();
                Console.WriteLine(testNodeList[1].Node.LedgerHelper._databaseName);
                foreach(var entry in entries2)
                {
                    Console.WriteLine(entry.ToString());
                }
                var entries = await testNodeList[0].Node.LedgerHelper.GetEntries();
                Console.WriteLine(testNodeList[0].Node.LedgerHelper._databaseName);
                foreach (var entry in entries)
                {
                    Console.WriteLine(entry.ToString());
                }
                Assert.IsTrue(entries == entries2, "Ledgers do not have same contents");

            });
            t.Start();
            t.Join();
            
        }

        /// <summary>
        /// In this test, we will test what will happen if node2 from testnetwork initiates an transaction
        /// Ultimately, every node from testnetwork and HomeNetwork-0123 should have the decree in their ledger
        /// This would be the path taken
        /// 
        /// testnetwork_2 -> sends TransactionProposal to testnetwork_1
        /// testnetwork_1 is leader and will do the paxos protocol
        /// 
        /// after finishing the protocol, testnetwork_1 will seek the leader of HomeNetwork-0123
        /// testnetwork_1 -> sends FindLeader-message to HomeNetwork-0123
        /// HomeNetwork-0123 -> sends testnetwork_1 a Leader-message, specifying the leader id, ip and port
        /// testnetwork_1 -> sends HomeNetwork-0123_1 a Transaction-message
        /// HomeNetwork-0123_1 does paxos
        /// HomeNetwork-0123_1 sends a TransactionSuccess-message to testnetwork_1.
        /// 
        /// In the test, we will check if everybody has the decree in their ledger.
        /// </summary>
        [TestMethod]
        public void TestTransactionDecree1()
        {
            addNodeFile("testnetwork", new List<(int, int)> { (1, 10000), (2, 10001) });
            addNodeFile("HomeNetwork-0123", new List<(int, int)> { (1, 10002), (2, 10003) });

            var testNodeList = new List<TestNode>() { new TestNode(10000, "testnetwork"),
                                                        new TestNode(10001, "testnetwork"),
                                                        new TestNode(10002, "HomeNetwork-0123"),
                                                        new TestNode(10003, "HomeNetwork-0123")};
            testNodeList.ForEach(node => node.start());

            
            for (int i = 0; i < 8; i++)
            {
                Console.WriteLine("[Test] Initialising... {0}", i);
                Thread.Sleep(500);
            }
            Console.WriteLine("[Test] Wakeup...");

            if (testNodeList.Exists(x => x.Node.PortNumber == 10001))
                testNodeList[1].Node.testinput("HomeNetwork-0123", "Transaction Testing...");
            Console.WriteLine("[Test] Wakeup...");
            for (int i = 0; i < 8; i++)
            {
                Console.WriteLine("[Test] Initialising... {0}", i);
                Thread.Sleep(500);
            }
            //Thread.Sleep(5000);//
            
            Thread t = new Thread(async () =>
            {
                List < List < PaxosCLI.Database.LedgerEntry >> ledgerentries = new();
                testNodeList.ForEach(node => Console.WriteLine(node.Node.LedgerHelper._databaseName));
                foreach(var (node, index) in testNodeList.Select((value, i) => (value, i)))
                {
                    Console.WriteLine(node.Node.LedgerHelper._databaseName);
                    ledgerentries.Add(await node.Node.LedgerHelper.GetEntries());
                    foreach (var entry in ledgerentries[index])
                    {
                        Console.WriteLine(entry.ToString());
                    }
                }
                //check if whole list is the same
                Assert.IsTrue(ledgerentries.Any(o => o != ledgerentries[0]) , "Ledgers do not have same contents");

            });
            t.Start();
            t.Join();
            
            Assert.IsTrue(true);


        }
        [TestMethod]
        public void TestSingleMessageDelivery()
        {
            addNodeFile("testnetwork", new List<(int, int)> { (1, 10000), (2, 10001) });
            TestNode testNode = new TestNode(10000, "testnetwork");
            testNode.start();
            IPEndPoint HomeAddress = new(GetLocalActiveIpAddress(), 10000);
            IPEndPoint FakeAddress = new(GetLocalActiveIpAddress(), 10001);
            UdpClient client = new(FakeAddress);

            for (int i = 0; i < 8; i++)
            {
                Console.WriteLine("[Test] Initialising... {0}", i);
                Thread.Sleep(500);
            }
            Console.WriteLine("[Test] Wakeup...");
            Assert.IsTrue(testNode.Node.Id == 1);
            var findleaderbuffer = Encoding.ASCII.GetBytes("1.1,FL,;");
            var leaderbuffer = Encoding.ASCII.GetBytes("1.1,L,FakeNetwork,1;1.1.1.1:10000");
            var transactionbuffer = Encoding.ASCII.GetBytes("1.1,T,FakeNetwork,1;FakeDecree");
            var transactionsuccessbuffer = Encoding.ASCII.GetBytes("1.1,TS,FakeNetwork,1;");
            var transactionProposal = Encoding.ASCII.GetBytes("1.1,TP,FakeNetwork;FakeDecree");

            client.Send(transactionbuffer, transactionbuffer.Length, HomeAddress);
            client.Send(transactionsuccessbuffer, transactionsuccessbuffer.Length, HomeAddress);
            client.Send(leaderbuffer, leaderbuffer.Length, HomeAddress);
            client.Send(transactionProposal, transactionProposal.Length, HomeAddress);
            client.Send(findleaderbuffer, findleaderbuffer.Length, HomeAddress);
            client.Send(transactionsuccessbuffer, transactionsuccessbuffer.Length, HomeAddress);

            //testNode.Node.testinput("", "This is a test 1234 beep boop");
        }
        [TestMethod]
        public void TestExecuteCrossNetwork()
        {
            Thread t = new Thread(async () =>
            {

            });
            t.Start();
        }
        [TestMethod]
        public void TestDoubleUdpClient()
        {
            List<UDPTest> testlist = new List<UDPTest>();
            for (int i = 0; i < 4; i++)
                testlist.Add(new UDPTest(GetLocalActiveIpAddress(), 10000 + i));
            Thread.Sleep(500);
            Console.WriteLine("[Test] Initialising...");
            for(int i = 0; i < 4; i++)
            {
                for(int j = 0; j < 4; j++)
                {
                    if (i == j) continue;//do not send to self
                    var msg = $"test[{i},{j}]";
                    testlist[i].sendMsg(msg, testlist[j].ipe);
                    Thread.Sleep(1); //wait for msg to arrive
                    Assert.IsTrue(testlist[j].lastReceive.Item1 == msg, "wrong message"); //&& 
                    Assert.IsTrue(testlist[j].lastReceive.Item2.ToString() == testlist[i].ipe.ToString(), $"ip-in:{testlist[j].lastReceive.Item2}, ip-expect:{testlist[i].ipe}");
                }
            }
        }
        
        class UDPTest
        {
            private UdpClient client;
            public IPEndPoint ipe;
            public Tuple<string, IPEndPoint>? lastReceive;
            public UDPTest(IPAddress ip, int port)
            {
                client = new();
                ipe = new(ip, port);
                client.Client.Bind(ipe);

                uint IOC_IN = 0x80000000;
                uint IOC_VENDOR = 0x18000000;
                uint SIO_UDP_CONNRESET = IOC_IN | IOC_VENDOR | 12;

                client.Client.IOControl((int)SIO_UDP_CONNRESET, new byte[] { Convert.ToByte(false) }, null);
                client.BeginReceive(new AsyncCallback(ReceiveMessageAsync), null);
            }
            private void ReceiveMessageAsync(IAsyncResult result)
            {
                IPEndPoint remoteIpEndPoint = new IPEndPoint(IPAddress.Any, 0);
                byte[] request = client.EndReceive(result, ref remoteIpEndPoint);
                client.BeginReceive(new AsyncCallback(ReceiveMessageAsync), null);
                Task.Factory.StartNew(async () => await ReceiveRequest(request, remoteIpEndPoint));
            }
            private async Task ReceiveRequest(byte[] request, IPEndPoint remoteIpEndPoint)
            {
                //Console.WriteLine(ipe.ToString() + " Received message from: " + remoteIpEndPoint.ToString() + "\nMessage content: " + Encoding.ASCII.GetString(request));
                lastReceive = new(Encoding.ASCII.GetString(request), remoteIpEndPoint);
            }
            public void sendMsg(string msg, IPEndPoint ip)
            {
                //Console.WriteLine(ipe.ToString() + " Sending message to: " + ip.ToString() + "\nMessage content: " + msg);
                Thread t = new Thread(async () =>
                {
                    try
                    {
                        var b = Encoding.ASCII.GetBytes(msg);
                        await client.SendAsync(b, b.Length, ip);
                    } catch (Exception ex) { }
                });
                t.Start();
                t.Join();
            }
        }
    }
}
