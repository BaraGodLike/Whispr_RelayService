using FluentMigrator;

namespace Migrator.Migrations;

[Migration(202605241910)]
public sealed class AddCleanupIndexesMigration : Migration
{
    public override void Up()
    {
        Create.Index("ix_pending_messages_created_at")
            .OnTable("pending_messages")
            .OnColumn("created_at").Ascending();

        Create.Index("ix_outbox_events_msg_id")
            .OnTable("outbox_events")
            .OnColumn("msg_id").Ascending();
    }

    public override void Down()
    {
        Delete.Index("ix_outbox_events_msg_id").OnTable("outbox_events");
        Delete.Index("ix_pending_messages_created_at").OnTable("pending_messages");
    }
}
