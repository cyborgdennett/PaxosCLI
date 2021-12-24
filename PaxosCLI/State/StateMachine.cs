using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PaxosCLI.Database;

namespace PaxosCLI.State
{
    /// <summary>
    /// Read the decrees after every update and then update the state of the system.
    /// </summary>
    class NetworkStateMachine
    {
        //name of sensor and then their last message
        Dictionary<string, LedgerEntry> states = new Dictionary<string,LedgerEntry>();
        /// <summary>
        /// Update own states and send transaction messages if needed
        /// </summary>
        public void update()
        {
            findStates();
        }
        /// <summary>
        /// Find the latest states of the nodes and save them to list of states
        /// </summary>
        public void findStates()
        {
            using (Ledger ledger = new Ledger())
            {
                foreach (string name in states.Keys)
                    states[name] = ledger.Entries.OrderBy(l => l.Id)
                    .Where(e => e.Decree.StartsWith(name))
                    .LastOrDefault();
            }               
        }
    }
}
