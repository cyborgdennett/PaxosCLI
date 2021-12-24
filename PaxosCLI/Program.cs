using PaxosCLI.NodeAgents;
using PaxosCLI;

class Program
{
    static void Main(string[] args)
    {

        Node node = null;

        try
        {
            node = new Node();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.ToString());
        }
    }
}

