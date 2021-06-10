/*

-- Create test SQL database with:

drop table if exists dbo.Price;
go

drop table if exists dbo.Occurrence;
go

create table dbo.Occurrence
(
    Id        int          not null identity,
    Title     nvarchar(80) not null,
    Timestamp rowversion   not null,
    constraint pk_Occurrence
        primary key clustered (Id)
);
create table dbo.Price
(
    OccurrenceId int        not null,
    Currency     char(3)    not null,
    Value        decimal    not null,
    Timestamp    rowversion not null,
    constraint pk_Price
        primary key clustered (OccurrenceId, Currency),
    constraint fk_Price_Occurrence
        foreign key (OccurrenceId)
        references dbo.Occurrence (Id)
);
go

 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EfCoreMsSqlData3
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            // Makes no difference

            // AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.LegacyRowVersionNullBehaviour", true);

            using (EventsDbContext db = new())
            {
                if ((await db.Occurrences.CountAsync()) == 0)
                {
                    // Note: no prices, therefore LEFT JOIN when included in query of occurrences will return nulls

                    for (var i = 0; i < 10; i++)
                    {
                        db.Occurrences.Add(new Occurrence { Title = "Test " + i });
                    }

                    await db.SaveChangesAsync();
                }
            }

            // This works

            using (EventsDbContext db = new())
            {
                foreach (var o in db.Occurrences.Include(o => o.Prices))
                {
                    Console.WriteLine(o.Title + " (" + o.Timestamp + ")");
                }
            }

            // This fails

            using (EventsDbContext db = new())
            {
                await foreach (var o in db.Occurrences.Include(o => o.Prices).AsAsyncEnumerable())
                {
                    Console.WriteLine(o.Title + " (" + o.Timestamp + ")");
                }
            }
        }
    }

    public class EventsDbContext : DbContext
    {
        private const string Connection = "Data Source=(local);Initial Catalog=EfCoreMsSqlData3;" +
            "Integrated Security=True;Connect Timeout=60;Encrypt=False;TrustServerCertificate=False;" +
            "ApplicationIntent=ReadWrite;MultiSubnetFailover=False";

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder
                .EnableDetailedErrors()
                .EnableSensitiveDataLogging()
                .UseSqlServer(Connection, options =>
                {
                    // Remove this and it works...

                    options.EnableRetryOnFailure();
                })
                .LogTo(Console.WriteLine);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Occurrence>()
                .ToTable("Occurrence")
                .HasKey(o => o.Id);

            modelBuilder.Entity<Occurrence>()
                .Property(o => o.Timestamp)
                .IsRowVersion();

            modelBuilder.Entity<Occurrence>()
                .HasMany(o => o.Prices)
                .WithOne(o => o.Occurrence)
                .HasForeignKey(p => p.OccurrenceId);

            modelBuilder.Entity<Price>()
                .ToTable("Price")
                .HasKey(p => new { p.OccurrenceId, p.Currency });

            modelBuilder.Entity<Price>()
                .Property(o => o.Timestamp)
                .IsRowVersion();
        }

        public DbSet<Occurrence> Occurrences { get; set; }
    }


    public abstract class PersistedObject
    {
        public byte[] Timestamp { get; set; }
    }

    public abstract class Entity<TId> : PersistedObject
        where TId : IEquatable<TId>
    {
        public TId Id { get; set; }
    }

    public class Occurrence : Entity<int>
    {
        public string Title { get; set; }

        public List<Price> Prices { get; set; }
    }

    public class Price : PersistedObject
    {
        public int OccurrenceId { get; set; }

        public string Currency { get; set; }

        public Occurrence Occurrence { get; set; }

        public decimal Value { get; set; }
    }
}
