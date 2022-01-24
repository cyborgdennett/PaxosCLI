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
                //Create node ip files
                var stringpath = Directory.GetCurrentDirectory();
                var nodespath = Path.Combine(stringpath, "Nodes");
                if (!Directory.Exists(nodespath))
                {
                    Directory.CreateDirectory(nodespath);
                }
                Thread.Sleep(5);
            }
            catch (Exception ex) { }
        }
        [TestCleanup()]
        public void cleanup()
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
            }
            catch (Exception ex) { }
        }
        public void addNodeFile(string network, List<(int, int)> idport)
        {
            var stringpath = Directory.GetCurrentDirectory();
            var nodespath = Path.Combine(stringpath, "Nodes");
            var filepath = Path.Combine(nodespath, network + ".csv");
            if (!System.IO.File.Exists(filepath))
            {
                File.Create(filepath);
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
            public void stop() => Thread.Abort();
        }

        private IPAddress GetLocalActiveIpAddress()
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
        public void TestProgram()
        {
            addNodeFile("testnetwork", new List<(int, int)> { (1, 10000), (2, 10001) });
            TestNode testNode = new TestNode(10000, "testnetwork");
            testNode.start();
            var stringpath = Directory.GetCurrentDirectory();
            var nodespath = Path.Combine(stringpath, "Nodes/testnetwork.csv");
            foreach(var line in File.ReadAllLines(nodespath))
            {
                Console.WriteLine(line);
            }
            
            for (int i = 0; i < 8; i++) {
                Console.WriteLine("[Test] Initialising... {0}", i);
                Thread.Sleep(500);
            }

            Console.WriteLine("[Test] Wakeup...");
            //thread.Join(new TimeSpan(0, 0, 2));
            var findleaderbuffer = Encoding.ASCII.GetBytes("1.1,FL,;");
            var leaderbuffer = Encoding.ASCII.GetBytes("1.1,L,FakeNetwork,1;1.1.1.1:10000");
            var transactionbuffer = Encoding.ASCII.GetBytes("1.1,T,FakeNetwork,1;FakeDecree");
            var transactionsuccessbuffer = Encoding.ASCII.GetBytes("1.1,TS,FakeNetwork,1;");
            var transactionProposal = Encoding.ASCII.GetBytes("1.1,TP,FakeNetwork;FakeDecree");

            IPEndPoint HomeAddress = new(GetLocalActiveIpAddress(), 10000);
            IPEndPoint FakeAddress = new(GetLocalActiveIpAddress(), 10001);
            UdpClient client = new(FakeAddress);

            client.Send(transactionbuffer, transactionbuffer.Length, HomeAddress);
            client.Send(transactionsuccessbuffer, transactionsuccessbuffer.Length, HomeAddress);
            client.Send(leaderbuffer, leaderbuffer.Length, HomeAddress);
            client.Send(transactionProposal, transactionProposal.Length, HomeAddress);
            client.Send(findleaderbuffer, findleaderbuffer.Length, HomeAddress);
            client.Send(transactionsuccessbuffer, transactionsuccessbuffer.Length, HomeAddress);

            testNode.Node.testinput("", "This is a test 1234 beep boop");
            Thread.Sleep(2000);

            //Assert.IsTrue(true);
            Console.WriteLine(testNode.Node);
            Assert.IsTrue(testNode.Node.Id == 1);
        }
        [TestMethod]
        public void TestDoubleUdpClient()
        {
            Console.WriteLine("haha");
            Assert.IsTrue(true);
        }
    }
}
