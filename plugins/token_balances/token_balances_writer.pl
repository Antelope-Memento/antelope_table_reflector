use strict;
use warnings;
use DBD::Pg qw(:pg_types);



sub token_balances_prepare
{
    my $args = shift;

    my $dbh = $main::db->{'dbh'};

    $main::db->{'token_balances_ins'} =
        $dbh->prepare('INSERT INTO CURRENCY_BAL (account_name, contract, currency, amount, decimals) ' .
                      'VALUES (?,?,?,?,?)');

    $main::db->{'token_balances_del'} =
        $dbh->prepare('DELETE FROM CURRENCY_BAL WHERE account_name=? AND contract=? AND currency=?');

    printf STDERR ("token_balances_writer.pl prepared\n");
}



sub token_balances_check_kvo
{
    my $kvo = shift;

    if( $kvo->{'table'} eq 'accounts' )
    {
        if( defined($kvo->{'value'}{'balance'}) and
            $kvo->{'scope'} =~ /^[a-z0-5.]+$/ )
        {
            return 1;
        }
    }
    return 0;
}


sub token_balances_row
{
    my $added = shift;
    my $kvo = shift;
    my $block_num = shift;
    
    if( $kvo->{'table'} eq 'accounts' )
    {
        if( defined($kvo->{'value'}{'balance'}) and
            $kvo->{'scope'} =~ /^[a-z0-5.]+$/ )
        {
            my $bal = $kvo->{'value'}{'balance'};
            if( $bal =~ /^([0-9.]+) ([A-Z]{1,7})$/ )
            {
                my $amount = $1;
                my $currency = $2;

                if( $added )
                {
                    my $decimals = 0;
                    my $pos = index($amount, '.');
                    if( $pos > -1 )
                    {
                        $decimals = length($amount) - $pos - 1;
                    }

                    $amount =~ s/\.//;

                    $main::db->{'token_balances_ins'}->execute($kvo->{'scope'}, $kvo->{'code'}, $currency, $amount, $decimals);
                }
                else
                {
                    $main::db->{'token_balances_del'}->execute($kvo->{'scope'}, $kvo->{'code'}, $currency);
                }
            }
        }
    }
}




push(@main::prepare_hooks, \&token_balances_prepare);
push(@main::check_kvo_hooks, \&token_balances_check_kvo);
push(@main::row_hooks, \&token_balances_row);

1;
