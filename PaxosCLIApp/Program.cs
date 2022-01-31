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

public class Program
{

    public static void Main(string[] args)
    {
        var p = new Program();
        foreach (string a in args)
        {
            if(a == "cleanup")
                p.startup();
            Console.WriteLine(a);
        }
        p.addNodeFile("testnetwork", new List<(int, int)> { (1, 10000), (2, 10001) });
        p.addNodeFile("HomeNetwork-0123", new List<(int, int)> { (1, 10002), (2, 10003) });

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


        while (true)
        {
            try
            {
                Console.WriteLine("Enter local node in list, network and decree msg.");
                var input = Console.ReadLine().Split(";");
                Console.WriteLine(input[0]);
                Console.WriteLine(input[1]);
                Console.WriteLine(input[2]);
                //input.ForEach(x => Console.WriteLine(x));
                testNodeList[Int16.Parse(input[0])].Node.ManualInput(input[2], input[1]);
            }
            catch (Exception ex) { }


        }
    }
    public Program() { }
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
            foreach (FileInfo file in files)
            {
                Console.WriteLine(file.FullName);
                File.Delete(file.FullName);
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
            File.Create(filepath).Dispose();
        }
        Thread.Sleep(5);
        using (System.IO.FileStream fs = File.OpenWrite(filepath))
        {
            var ip = GetLocalActiveIpAddress();
            StringBuilder ipstring = new StringBuilder();
            foreach ((int id, int port) in idport)
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
            Thread = new Thread(async () =>
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

}
