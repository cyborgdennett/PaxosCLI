using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace PaxosCLITest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            Assert.IsTrue(false);
        }
        [TestMethod]
        public void TestMethod2()
        {
            Assert.IsTrue(true);
        }
        [TestMethod]
        public void TestMultiScreen()
        {
            ProcessStartInfo psi = new ProcessStartInfo("cmd.exe")
            {
                RedirectStandardError = true,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                UseShellExecute = true,
                Arguments = $"/c start cmd.exe",
                CreateNoWindow = false
            };

            Process p = Process.Start(psi);

            StreamWriter sw = p.StandardInput;
            StreamReader sr = p.StandardOutput;

            sw.WriteLine("Hello world!");
            Thread.Sleep(2000);
            sw.WriteLine("Hello world!");
            //sr.Close();
            Assert.IsTrue(true);
        }
    }
}