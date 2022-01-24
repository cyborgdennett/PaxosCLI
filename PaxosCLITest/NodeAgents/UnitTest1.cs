using Microsoft.VisualStudio.TestTools.UnitTesting;
using PaxosCLI.NodeAgents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PaxosCLI.NodeAgents.Tests
{
    [TestClass()]
    public class ClusterTests
    {
        [TestMethod()]
        public void HasMajorityOfTest()
        {
            List<Node> nodes1 = new() { new Node(1, System.Net.IPAddress.Parse("123.123.123.123"),1000), new Node(2, System.Net.IPAddress.Parse("123.123.123.124"), 1001), new Node(1, System.Net.IPAddress.Parse("123.123.123.125"), 1002) };
            List<Node> nodes2 = new() { new Node(1, System.Net.IPAddress.Parse("123.123.123.123"), 1000), new Node(2, System.Net.IPAddress.Parse("123.123.123.124"), 1001), new Node(4, System.Net.IPAddress.Parse("123.123.123.125"), 1005) };
            Cluster c = new(nodes1);
            Cluster o = new(nodes2);
            
            Assert.IsTrue(c.HasMajorityOf(o),"2/3 nodes correspond, so this should be true");
            Node node = new Node(1, System.Net.IPAddress.Parse("123.123.123.123"), 9999); //change the port of the first element
            o.TryRemove(1, out node); // remove one 
            Assert.IsFalse(c.HasMajorityOf(o), "1/3 nodes correspond, so this should be false");
        }
    }
}