# Antelope contract tracker


```
cd /var/local
wget https://github.com/EOSChronicleProject/eos-chronicle/releases/download/v2.3/eosio-chronicle-2.3-Clang-11.0.1-ubuntu22.04-x86_64.deb
apt install ./eosio-chronicle-2.3-Clang-11.0.1-ubuntu22.04-x86_64.deb
cp /usr/local/share/chronicle_receiver\@.service /etc/systemd/system/
systemctl daemon-reload

apt install -y git g++ make libdbd-pg-perl cpanminus libjson-xs-perl libjson-perl libdbi-perl libdatetime-format-pg-perl libcryptx-perl
cpanm --notest Net::WebSocket::Server

git clone https://github.com/Antelope-Memento/antelope_table_reflector.git /opt/antelope_table_reflector
cd /opt/antelope_table_reflector
cp systemd/*.service /etc/systemd/system/
systemctl daemon-reload

psql "postgres://DBUSER:DBPASSWORD@10.0.3.1/DBNAME" <sql/table_reflector_timescale.sql
psql "postgres://DBUSER:DBPASSWORD@10.0.3.1/DBNAME" <plugins/token_balances/token_balances_timescale.sql

echo 'DBWRITER_OPTS="--id=1 --port=8010 --database=DBNAME --dbhost=10.0.3.1 --dbuser=DBUSER --dbpw=DBPASSWORD --plugin=/opt/antelope_table_reflector/plugins/token_balances/token_balances_writer.pl"' >/etc/default/table_reflector_DBNAME

systemctl enable table_reflector_dbwriter@DBNAME
systemctl start table_reflector_dbwriter@DBNAME

mkdir -p /srv/table_reflector_DBNAME/chronicle-config
cat >/srv/table_reflector_DBNAME/chronicle-config/config.ini <<'EOT'
host = 10.0.3.1
port = 8185
mode = scan
skip-block-events = true
plugin = exp_ws_plugin
exp-ws-host = 127.0.0.1
exp-ws-port = 8010
exp-ws-bin-header = true
skip-traces = true
skip-account-info = true
EOT

systemctl enable chronicle_receiver@table_reflector_DBNAME
systemctl start chronicle_receiver@table_reflector_DBNAME
```
