use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use DBD::Pg qw(:pg_types);
use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;
use Time::HiRes qw (time);
use Time::Local 'timegm_nocheck';
use Crypt::Digest::SHA256 qw(sha256);

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;

$| = 1;

my $port = 8001;
my $ack_every = 100;

my $sourceid;

my $db_name;
my $db_host;
my $db_port = 5432;
my $db_user;
my $db_password;
my @plugins;
my %pluginargs;

my $ok = GetOptions
    (
     'ack=i'     => \$ack_every,
     'id=i'      => \$sourceid,
     'port=i'    => \$port,
     'ack=i'     => \$ack_every,
     'database=s' => \$db_name,
     'dbhost=s'  => \$db_host,
     'dbport=s'  => \$db_port,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'plugin=s'  => \@plugins,
     'parg=s%'   => \%pluginargs,
    );


if( not $ok or not $sourceid or not ($sourceid == 1 or $sourceid == 2) or
    not $db_name or not $db_host or not $db_user or not $db_password or
    or scalar(@plugins) == 0 or scalar(@ARGV) > 0)
{
    print STDERR "Usage: $0 --id=N --database=DB --dbhost=HOST --dbuser=USR --dbpw=PW --plugin=PLUGIN [options...]\n",
        "The utility opens a WS port for Chronicle to send data to.\n",
        "Options:\n",
        "  --id=N             source instance identifier (1 or 2)\n",
        "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
        "  --ack=N            \[$ack_every\] Send acknowledgements every N blocks\n",
        "  --dbport=N         \[$db_port\] database port\n"
        "  --plugin=FILE.pl   plugin program for custom processing\n",
        "  --parg KEY=VAL     plugin configuration options\n";
    exit 1;
}


our @prepare_hooks;
our @row_hooks;
our @block_hooks;
our @ack_hooks;
our @fork_hooks;
our @lib_hooks;

foreach my $plugin (@plugins)
{
    require($plugin);
}


my $dsn = 'dbi:Pg:dbname=' . $db_name . ';host=' . $db_host . ';port=' . $db_port;

my $db;
my $json = JSON->new->canonical;


my $confirmed_block = 0;
my $unconfirmed_block = 0;
my $irreversible = 0;

my $i_am_master;
my $retired_on = 0; # timestamp of last time losing master status

my $just_committed = 1;
my $locked = 0;

my $blocks_counter = 0;
my $rows_counter = 0;
my $counter_start = time();

my $total_rows = 0;
my $total_next_report = 100000;

getdb();

{
    # sanity check, there should be only one master
    my $sth = $db->{'dbh'}->prepare
        ('SELECT sourceid, block_num, irreversible FROM SYNC WHERE is_master=1');
    $sth->execute();
    my $masters = $sth->fetchall_arrayref();
    if( scalar(@{$masters}) == 0 )
    {
        die("no master is defined in SYNC table\n");
    }
    elsif( scalar(@{$masters}) > 1 )
    {
        die("more than one master is defined in SYNC table\n");
    }

    $sth = $db->{'dbh'}->prepare
        ('SELECT block_num, irreversible, is_master FROM SYNC WHERE sourceid=?');
    $sth->execute($sourceid);
    my $r = $sth->fetchall_arrayref();
    if( scalar(@{$r}) == 0 )
    {
        die("sourceid=$sourceid is not defined in SYNC table\n");
    }

    $confirmed_block = $r->[0][0];
    $unconfirmed_block = $confirmed_block;
    $irreversible = $r->[0][1];
    $i_am_master = $r->[0][2];
    printf STDERR ("Starting from confirmed_block=%d, irreversible=%d, sourceid=%d, is_master=%d\n",
                   $confirmed_block, $irreversible, $sourceid, $i_am_master);

    if( not $i_am_master )
    {
        # make sure the master is running
        if( $masters->[0][1] == 0 or $masters->[0][2] == 0 )
        {
            die("sourceid=" . $masters->[0][0] . " is defined as master, but it has not started yet\n");
        }
    }
}


foreach my $hook (@prepare_hooks)
{
    &{$hook}(\%pluginargs);
}


Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'binary' => sub {
                my ($conn, $msg) = @_;
                my ($msgtype, $opts, $js) = unpack('VVa*', $msg);
                my $data = eval {$json->decode($js)};
                if( $@ )
                {
                    print STDERR $@, "\n\n";
                    print STDERR $js, "\n";
                    exit;
                }

                if( $i_am_master and $just_committed )
                {
                    # verify that I am still the master
                    $db->{'sth_am_i_master'}->execute($sourceid);
                    my $r = $db->{'sth_am_i_master'}->fetchall_arrayref();
                    if( not $r->[0][0] )
                    {
                        printf STDERR ("I am no longer the master (sourceid=%d)\n", $sourceid);
                        $i_am_master = 0;
                        $retired_on = time();
                    }
                    $just_committed = 0;
                }

                my $ack = process_data($msgtype, $data, \$js);
                if( $ack >= 0 )
                {
                    $conn->send_binary(sprintf("%d", $ack));
                    print STDERR "ack $ack\n";
                }
            },
            'disconnect' => sub {
                print STDERR "Disconnected\n";
                $db->{'dbh'}->rollback();
            },

            );
    },
    )->start;


sub process_data
{
    my $msgtype = shift;
    my $data = shift;
    my $jsptr = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";

        if( $i_am_master )
        {
            foreach my $hook (@fork_hooks)
            {
                &{$hook}($start_block);
            }
        }

        getdb();
        $db->{'dbh'}->commit();
        $just_committed = 1;

        if( $confirmed_block <= $irreversible )
        {
            $unconfirmed_block = $block_num - 1;
            return $unconfirmed_block;
        }

        # roll back all changes down to the fork block

        my $n_deleted = 0;
        my $n_restored = 0;

        if( $i_am_master )
        {
            $db->{'dbh'}->do('LOCK TABLE WRITER_LOCK IN EXCLUSIVE MODE');
        }

        $db->{'sth_get_jupdates'}->execute($block_num);
        while( my $r = $db->{'sth_get_jupdates'}->fetchrow_arrayref() )
        {
            my ($j_block_num, $op, $selector, $prev_jsdata) = @{$r};

            my $kvo;

            if( $i_am_master )
            {
                if( defined($prev_jsdata) )
                {
                    my $rollback = eval {$json->decode($prev_jsdata)};
                    if( $@ )
                    {
                        print STDERR $@, "\n\n";
                        print STDERR $r->[0], "\n";
                        exit;
                    }

                    $kvo = $rollback->{'kvo'};

                    # delete the application row
                    if( $i_am_master )
                    {
                        foreach my $hook (@row_hooks)
                        {
                            &{$hook}(0, $kvo);
                        }
                    }
                }
            }

            $db->{'sth_del_jcurrent'}->execute($selector);
            $n_deleted++;

            if( $op == 2 or $op == 3 )
            {
                # updated or deleted row, restore it back

                if( $i_am_master )
                {
                    if( defined($kvo) )
                    {
                        foreach my $hook (@row_hooks)
                        {
                            &{$hook}(1, $kvo);
                        }
                    }
                }

                $db->{'sth_ins_jcurrent'}->execute($selector, $prev_jsdata);
                $n_restored++;
            }

            $confirmed_block = $j_block_num - 1;
        }

        $db->{'sth_fork_jupdates'}->execute($block_num);
        $db->{'dbh'}->commit();
        $just_committed = 1;

        printf STDERR ("Deleted %d, restored %d rows, rolled back to block %d\n",
                       $n_deleted, $n_restored, $confirmed_block);

        $unconfirmed_block = $confirmed_block;
        return ($block_num-1);
    }
    elsif( $msgtype == 1007 ) # CHRONICLE_MSGTYPE_TBL_ROW
    {
        my $block_num = $data->{'block_num'};
        if( $block_num <= $confirmed_block and $confirmed_block <= $irreversible )
        {
            return -1;
        }

        my $kvo = $data->{'kvo'};
        if( ref($kvo->{'value'}) eq 'HASH' )
        {
            my $added = ($data->{'added'} eq 'true') ? 1:0;

            if( $i_am_master )
            {
                if( not $locked )
                {
                    $db->{'dbh'}->do('LOCK TABLE WRITER_LOCK IN EXCLUSIVE MODE');
                    $locked = 1;
                }

                foreach my $hook (@row_hooks)
                {
                    &{$hook}(0, $kvo);
                }

                if( $added )
                {
                    foreach my $hook (@row_hooks)
                    {
                        &{$hook}(1, $kvo);
                    }
                }
            }

            # journal the update in rollback tables

            my $selector_input = join(':', $kvo->{'code'}, $kvo->{'scope'}, $kvo->{'table'},
                                      sprintf('%.8LX', $kvo->{'primary_key'}));
            my $selector = sha256($selector_input);

            if( $irreversible > 0 ) # it is zero at initial import, and we don't want to pollute the journal with that.
            {
                my $prev_jsdata;
                my $op;

                $db->{'sth_get_jcurrent'}->execute($selector);
                my $prevrow = $db->{'sth_get_jcurrent'}->fetchall_arrayref();
                if( scalar(@{$prevrow}) > 0 )
                {
                    $prev_jsdata = $prevrow->[0][0];
                    if( $added )
                    {
                        # this is an update of existing row
                        $op = 2;
                    }
                    else
                    {
                        # the row is deleted
                        $op = 3;
                    }
                }
                else
                {
                    if( $added )
                    {
                        # this is a new row
                        $op = 1;
                    }
                    else
                    {
                        printf STDERR ("Database possibly corrupted, row deleted but did not exist: %s\n",
                                       $selector_input);
                        # the row is deleted
                        $op = 3;
                    }
                }

                $db->{'sth_ins_jupdates'}->execute($block_num, $op, $selector, $prev_jsdata, ${$jsptr});
            }

            $db->{'sth_del_jcurrent'}->execute($selector);
            if( $added )
            {
                $db->{'sth_ins_jcurrent'}->execute($selector, ${$jsptr});
            }

            $rows_counter++;
            $total_rows++;
            if( $total_rows >= $total_next_report )
            {
                $total_next_report += 100000;
                printf STDERR ("Imported rows: %d, current block: %d\n", $total_rows, $block_num);
            }
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        $blocks_counter++;
        my $block_num = $data->{'block_num'};
        my $block_time = $data->{'block_timestamp'};
        $block_time =~ s/T/ /;
        my $last_irreversible = $data->{'last_irreversible'};

        if( $block_num > $unconfirmed_block+1 )
        {
            printf STDERR ("WARNING: missing blocks %d to %d\n", $unconfirmed_block+1, $block_num-1);
        }

        if( $block_num > $last_irreversible + 100 )
        {
            $ack_every = 1;
        }

        if( $last_irreversible > $irreversible )
        {
            # LIB has moved, so delete old entries from JUPDATES
            $irreversible = $last_irreversible;
            foreach my $hook (@lib_hooks)
            {
                &{$hook}($irreversible);
            }

            $db->{'sth_del_old_jupdates'}->execute();
        }

        $unconfirmed_block = $block_num;

        if( $unconfirmed_block <= $confirmed_block )
        {
            # we are catching up through irreversible data, and this block was already stored in DB
            return $unconfirmed_block;
        }

        if( $i_am_master )
        {
            foreach my $hook (@block_hooks)
            {
                &{$hook}($block_num, $last_irreversible);
            }
        }

        if( $unconfirmed_block - $confirmed_block >= $ack_every )
        {
            if( $i_am_master )
            {
                foreach my $hook (@ack_hooks)
                {
                    &{$hook}($block_num);
                }
            }

            $db->{'sth_upd_sync_head'}->execute($block_num, $block_time, $last_irreversible, $sourceid);
            $db->{'dbh'}->commit();
            $just_committed = 1;
            $locked = 0;
            $confirmed_block = $unconfirmed_block;

            my $gap = 0;
            {
                my ($year, $mon, $mday, $hour, $min, $sec, $msec) =
                    split(/[-:.T]/, $data->{'block_timestamp'});
                my $epoch = timegm_nocheck($sec, $min, $hour, $mday, $mon-1, $year);
                $gap = (time() - $epoch)/3600.0;
            }

            if( not $i_am_master and $block_num > $last_irreversible and time() > $retired_on + 60 )
            {
                # check if the master is still alive

                $db->{'sth_check_sync_health'}->execute();
                my $my_upd;
                my $my_irrev;
                my $master_upd;
                my $master_irrev;
                my $old_master;

                while( my $r = $db->{'sth_check_sync_health'}->fetchrow_hashref('NAME_lc') )
                {
                    if( $r->{'sourceid'} == $sourceid )
                    {
                        $my_upd = $r->{'upd'};
                        $my_irrev = $r->{'irreversible'};
                    }
                    elsif( $r->{'is_master'} )
                    {
                        $master_upd = $r->{'upd'};
                        $master_irrev = $r->{'irreversible'};
                        $old_master = $r->{'sourceid'};
                    }
                }

                if( not defined($my_upd) or not defined($my_irrev) or
                    not defined($master_upd) or not defined($master_irrev) or
                    not defined($old_master) )
                {
                    die('SYNC corrupted');
                }

                if( $master_irrev < $my_irrev - 120 and $master_upd > $my_upd + 120 and $my_upd < 10 )
                {
                    printf STDERR ("Master process (sourceid=%i) stopped, taking over the master role\n", $old_master);
                    printf STDERR ("my_upd=%d, my_irrev=%d, master_upd=%d, master_irrev=%d\n",
                                   $my_upd, $my_irrev, $master_upd, $master_irrev);

                    $db->{'dbh'}->do('UPDATE SYNC SET is_master=0 WHERE sourceid != ?', undef, $sourceid);
                    $db->{'dbh'}->do('UPDATE SYNC SET is_master=1 WHERE sourceid = ?', undef, $sourceid);
                    $db->{'dbh'}->commit();

                    $db->{'dbh'}->do('LOCK TABLE WRITER_LOCK IN EXCLUSIVE MODE');

                    printf STDERR ("Sleeping 5 seconds\n");
                    sleep(5);
                    printf STDERR ("Rolling back old master data\n");

                    my $rolled_out = 0;
                    my $rolled_in = 0;

                    # roll back the data through old master journal up to old master's irreversible
                    my $sth = $db->{'dbh'}->prepare('SELECT block_num, op, selector, prev_jsdata ' .
                                                    'FROM JUPDATES_' . $old_master . ' WHERE block_num >= ? ORDER BY seqnum DESC');
                    $sth->execute($master_irrev);
                    while( my $r = $sth->fetchrow_arrayref() )
                    {
                        my ($j_block_num, $op, $selector, $prev_jsdata) = @{$r};

                        my $kvo;

                        if( defined($prev_jsdata) )
                        {
                            my $rollback = eval {$json->decode($prev_jsdata)};
                            if( $@ )
                            {
                                print STDERR $@, "\n\n";
                                print STDERR $r->[0], "\n";
                                exit;
                            }

                            $kvo = $rollback->{'kvo'};

                            # delete the application row
                            foreach my $hook (@row_hooks)
                            {
                                &{$hook}(0, $kvo);
                            }
                        }

                        if( defined($pk) )
                        {
                            my $sth_del = $db->{'dbh'}->prepare('DELETE FROM ' . $tblname . ' WHERE ' . $pk . '=?');
                            $sth_del->execute($values->{$pk});
                            $rolled_out++;
                        }

                        if( $op == 2 or $op == 3 )
                        {
                            # updated or deleted row, restore it back

                            if( defined($kvo) )
                            {
                                foreach my $hook (@row_hooks)
                                {
                                    &{$hook}(1, $kvo);
                                }
                            }
                        }
                    }

                    # restore the data from our journal
                    $sth = $db->{'dbh'}->prepare('SELECT block_num, op, selector, new_jsdata ' .
                                                 'FROM JUPDATES_' . $sourceid . ' WHERE block_num >= ? ORDER BY seqnum ASC');

                    $sth->execute($master_irrev);
                    while( my $r = $sth->fetchrow_arrayref() )
                    {
                        my ($j_block_num, $op, $selector, $new_jsdata) = @{$r};

                        my $newval = eval {$json->decode($new_jsdata)};
                        if( $@ )
                        {
                            print STDERR $@, "\n\n";
                            print STDERR $r->[0], "\n";
                            exit;
                        }

                        my $kvo = $newval->{'kvo'};
                        if( defined($kvo) )
                        {
                            # delete the application row
                            foreach my $hook (@row_hooks)
                            {
                                &{$hook}(0, $kvo);
                            }

                            $rolled_in++;

                            if( $op == 1 or $op == 2 )
                            {
                                # new or updated row
                                foreach my $hook (@row_hooks)
                                {
                                    &{$hook}(1, $kvo);
                                }
                            }
                        }
                    }

                    $db->{'dbh'}->commit();
                    $i_am_master = 1;
                    $just_committed = 1;
                    printf STDERR ("Rolled back old master data (%d rows) and rolled in new data (%d)\n",
                                   $rolled_out, $rolled_in);
                }
            }

            my $period = time() - $counter_start;
            printf STDERR ("%s - blocks/s: %8.2f, rows/block: %8.2f, rows/s: %8.2f, gap: %8.4fh, ",
                           ($i_am_master?'M':'S'),
                           $blocks_counter/$period, $rows_counter/$blocks_counter,
                           $rows_counter/$period, $gap);
            $counter_start = time();
            $blocks_counter = 0;
            $rows_counter = 0;

            return $confirmed_block;
        }
    }
    return -1;
}



sub getdb
{
    if( defined($db) and defined($db->{'dbh'}) and $db->{'dbh'}->ping() )
    {
        return;
    }

    my $dbh = $db->{'dbh'} = DBI->connect($dsn, $db_user, $db_password,
                                          {'RaiseError' => 1, AutoCommit => 0});
    die($DBI::errstr) unless $dbh;

    $db->{'sth_upd_sync_head'} = $dbh->prepare
        ('UPDATE SYNC SET block_num=?, block_time=?, irreversible=?, last_updated=NOW() WHERE sourceid=?');

    $db->{'sth_ins_jcurrent'} = $dbh->prepare('INSERT INTO JCURRENT_' . $sourceid . ' (selector, jsdata) VALUES (?,?)');
    $db->{'sth_ins_jcurrent'}->bind_param(1, undef, { 'pg_type' => PG_BYTEA });
    $db->{'sth_ins_jcurrent'}->bind_param(2, undef, { 'pg_type' => PG_BYTEA });

    $db->{'sth_del_jcurrent'} = $dbh->prepare('DELETE FROM JCURRENT_' . $sourceid . ' WHERE selector=?');
    $db->{'sth_del_jcurrent'}->bind_param(1, undef, { 'pg_type' => PG_BYTEA });

    $db->{'sth_get_jcurrent'} = $dbh->prepare('SELECT jsdata FROM JCURRENT_' . $sourceid . ' WHERE selector=?');
    $db->{'sth_get_jcurrent'}->bind_param(1, undef, { 'pg_type' => PG_BYTEA });

    $db->{'sth_ins_jupdates'} =
        $dbh->prepare('INSERT INTO JUPDATES_' . $sourceid . ' (block_num, op, selector, prev_jsdata, new_jsdata) ' .
                      'VALUES (?,?,?,?,?)');
    $db->{'sth_ins_jupdates'}->bind_param(3, undef, { 'pg_type' => PG_BYTEA });
    $db->{'sth_ins_jupdates'}->bind_param(4, undef, { 'pg_type' => PG_BYTEA });
    $db->{'sth_ins_jupdates'}->bind_param(5, undef, { 'pg_type' => PG_BYTEA });

    $db->{'sth_get_jupdates'} = $dbh->prepare('SELECT block_num, op, selector, prev_jsdata ' .
                                              'FROM JUPDATES_' . $sourceid . ' WHERE block_num >= ? ORDER BY seqnum DESC');

    $db->{'sth_del_old_jupdates'} =
        $dbh->prepare('DELETE FROM JUPDATES_' . $sourceid . ' WHERE block_num < (SELECT MIN(irreversible) FROM SYNC)');

    $db->{'sth_fork_jupdates'} = $dbh->prepare('DELETE FROM JUPDATES_' . $sourceid . ' WHERE block_num >= ?');

    $db->{'sth_check_sync_health'} =
        $dbh->prepare('SELECT sourceid, irreversible, is_master, EXTRACT(epoch FROM NOW() - last_updated) AS upd ' .
                      'FROM SYNC');

    $db->{'sth_am_i_master'} = $dbh->prepare('SELECT is_master FROM SYNC WHERE sourceid=?');
}
