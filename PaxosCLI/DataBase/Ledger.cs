
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace PaxosCLI.Database;

/// <summary>
/// This class is used to read/write data from the database, using Entity Framework.
/// </summary>
public class Ledger : DbContext
{
    public DbSet<LedgerEntry> Entries { get; set; } //All of the entries, written due to balloting.
    public DbSet<PaxosProgress> Progress { get; set; } //The progress required to be kept track of by the priest. Contains only one record
    public DbSet<NetworkNodeIps> NodeAdresses { get; set; } //Stores the ips.
    private string filename = "ledger.db";
    public Ledger(string db = "ledger.db")
    {
        filename = db;
        Database.EnsureCreated();
    }
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        // At the moment the SQLite database provider is used to store data.
        // It can be changed to many kinds of databases. See https://docs.microsoft.com/en-us/ef/core/providers/
        //optionsBuilder.UseSqlite("Data Source=ledger.db");
        optionsBuilder.UseSqlite("Data Source=" + filename);
    }

    /// <summary>
    /// Sets the data of the Paxos progress to the default values mentioned in the Part-Time Parliament Appendix/pseudocode
    /// </summary>
    /// <param name="modelBuilder"></param>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PaxosProgress>()
            .HasData(new PaxosProgress
            {
                Id = 1,
                LastTried = decimal.MinValue,
                NextBal = decimal.MinValue,
                PrevBal = decimal.MinValue,
                PrevDec = new byte[] { }
            });
    }
}

/// <summary>
/// This class contains the information on which consensus has been reached.
/// Because it is only a proof of concept, the decision was made to reach agreement over solely a string (Decree).
/// </summary>
[Table("LedgerEntry")]
public class LedgerEntry
{
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public long Id { get; set; }
    public string Decree { get; set; }

    public override string ToString()
    {
        return string.Format("LedgerEntry: Id= {0}, Decree={1}", Id, Decree);
    }
}

/// <summary>
/// Contains the important progress information the priest had to keep track of in the parliament.
/// </summary>
[Table("PaxosProgress")]
public class PaxosProgress
{
    public int Id { get; set; }
    public decimal LastTried { get; set; }
    public decimal NextBal { get; set; }
    public decimal PrevBal { get; set; }
    public byte[] PrevDec { get; set; }
}

/// <summary>
/// Contains the Ip address of the nodes.
/// </summary>
[Table("NetworkNodeIps")]
public class NetworkNodeIps
{
    public int Id { get; set; }
    public string Ip { get; set; }
    public int Port { get; set; }
}

