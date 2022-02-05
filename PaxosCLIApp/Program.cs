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
using System.Diagnostics;

public class Program
{

    public static void Main(string[] args)
    {

        string[] arg = { };
        if (args.Length != 0)
        {
            Console.WriteLine(args[0]);
            arg = args[0].Split(";");
        }

        if (arg.ElementAtOrDefault(0) == "newnode")
        {
            Console.Title = arg.ElementAtOrDefault(2) + arg.ElementAtOrDefault(1);
            TestNode t = new TestNode(Int16.Parse(arg[1]),arg[2]);
            t.start();
            while (true)
            {
                try
                {
                    Console.WriteLine("Enter decree message. For transaction, end message with ';network' where network is target network.");
                    var input = Console.ReadLine().Split(";");
                    if(input.Length == 1)
                        t.Node.ManualInput(input[0]);
                    else
                        t.Node.ManualInput(input[0], input[1]);
                }
                catch (Exception ex) { }
            }
        }

        var p = new Program();

        p.startup();
        p.addNodeFile("testnetwork", new List<(int, int)> { (1, 10000) });
        p.addNodeFile("testnetwork", new List<(int, int)> { (2, 10001) });
        p.addNodeFile("HomeNetwork-0123", new List<(int, int)> { (3, 10002) });
        p.addNodeFile("HomeNetwork-0123", new List<(int, int)> { (4, 10003) });
        //p.addNodeFile("testnetwork", new List<(int, int)> { (1, 10000) }, IPAddress.Parse("145.137.127.72"));
        //p.addNodeFile("testnetwork", new List<(int, int)> { (2, 10001) }, IPAddress.Parse("145.137.127.72"));
        //p.addNodeFile("HomeNetwork-0123", new List<(int, int)> { (1, 10002) }, IPAddress.Parse("145.137.127.72"));
        //p.addNodeFile("HomeNetwork-0123", new List<(int, int)> { (2, 10003) }, IPAddress.Parse("145.137.127.72"));

        var testNodeList = new List<(int, string)>() { (10000, "testnetwork"),
                                                    (10001, "testnetwork"),
                                                    (10002, "HomeNetwork-0123"),
                                                    (10003, "HomeNetwork-0123")
                                                    };

        List<Process> process = new();
        testNodeList.ForEach(x => process.Add(p.StartConsole(x.Item1, x.Item2)));
        
        Console.CancelKeyPress += delegate {
            //cleanup methods
            Console.WriteLine("Cleaning");
            process.ForEach(p => p.CloseMainWindow());
            process.ForEach(p => p.Close());
            Console.WriteLine("Closing");
        };

        for (int i = 0; i < 8; i++)
        {
            Console.WriteLine("[Test] Initialising... {0}", i);
            Thread.Sleep(500);
        }
        Console.WriteLine("[Test] Wakeup...");

        try
        {
            Task.Run(() => 
            {
                while (process.Count > 0)
                {

                    foreach (Process proc in process)
                    {
                        if (proc.HasExited || proc.MainWindowTitle == "")
                        {
                            Console.WriteLine(proc.Id + " Has stopped");
                            process.Remove(proc);
                            break;
                        }
                    }
                    Thread.Sleep(1000);
                }
                Console.WriteLine("all nodes stopped");
            });
            

            while (process.Count > 0)
            {
                var i = Console.ReadLine();
                if (i == "") continue;
                var strings = i.Split(" ");
                if (strings.ElementAtOrDefault(0) == "stop")
                {
                        
                    if (Int16.TryParse(strings.ElementAtOrDefault(1), out var n)
                        && n >= 0 && n < process.Count)
                    {
                        string name = process[n].MainWindowTitle;
                        Console.WriteLine("Stopping " + name);
                        Process pp = process[n];
                        process[n].CloseMainWindow();
                        process[n].Close();
                        try { process[n].Kill(); }
                        catch { }
                        process.Remove(process[n]);
                        Console.WriteLine("Stopped " + name);
                    }
                        
                }
                else if(strings.ElementAtOrDefault(0) == "start")
                {
                    if (Int16.TryParse(strings.ElementAtOrDefault(1), out var n)
                        && n >= 0 && n < process.Count)
                    {
                        string name = process[n].MainWindowTitle;
                        Console.WriteLine("Stopping " + name);
                        Process pp = process[n];
                        process[n].CloseMainWindow();
                        process[n].Close();
                        try { process[n].Kill(); }
                        catch { }
                        process.Remove(process[n]);
                        Console.WriteLine("Stopped " + name);
                    }
                }
            }

        }catch (Exception ex)
        {
            Console.WriteLine("An error occurred. Cleaning");
            process.ForEach(p => p.CloseMainWindow());
            process.ForEach(p => p.Close());
            Console.WriteLine("Closing");
        }
    }
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
    public void addNodeFile(string network, List<(int, int)> idport, IPAddress ipa = null)
    {
        var stringpath = Directory.GetCurrentDirectory();
        var nodespath = Path.Combine(stringpath, "Nodes");
        var filepath = Path.Combine(nodespath, network + ".csv");
        if (!System.IO.File.Exists(filepath))
        {
            File.Create(filepath).Dispose();
        }
        Thread.Sleep(5);
        using (System.IO.StreamWriter fs = File.AppendText(filepath))
        {
            var ip = ipa == null ? GetLocalActiveIpAddress() : ipa;
            StringBuilder ipstring = new StringBuilder();
            foreach ((int id, int port) in idport)
            {
                ipstring.Append("\n" + id.ToString() + "," + ip.ToString() + "," + port.ToString());
            }
            fs.Write(ipstring);
            fs.Close();
        }
    }
    public class TestNode
    {
        public volatile Node Node;
        public Thread Thread;
        int port { get; set; }
        string networkName { get; set; }
        public void start()
        {
            //Thread = new Thread(() =>
            //{
                try
                {
                    Node = new Node(networkName, port);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            //});
            //Thread.Start();
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

    public Process StartConsole(int port, string network)
    {
        ProcessStartInfo psi = new ProcessStartInfo("cmd.exe")
        {
            UseShellExecute = true,
            Arguments = $"/c dotnet run newnode;{port};{network}",
            CreateNoWindow = false
        };
        Console.WriteLine(psi.Arguments);
        Process p = Process.Start(psi);
        p.EnableRaisingEvents = true;

        return p;
    }

}
