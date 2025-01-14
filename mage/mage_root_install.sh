# create firewall rule entitled mage or mage-access, or something to that effect:
#   Logs: Off
#   Network: default
#   Priority: 65534
#   Direction: Ingress
#   Action on match: Allow
#   Targets: All instances in the network
#   Source filter: IPv4 ranges
#   Source IPv4 ranges: 0.0.0.0/0, <VM external ipv4 address>
#   Second source filter: None
#   Destination filter: None
#   Protocols and ports: Specified protocols and ports
#       TCP: 6789
sudo apt update
sudo apt install python3-full python3-pip
sudo rm /usr/lib/python3.11/EXTERNALLY-MANAGED
sudo pip install google-cloud 
sudo pip install google-cloud-bigquery
sudo pip install certifi
sudo pip install mage-ai
mage start < PROJECT >
# enter http://<Google Cloud VM external IP address>:6789 into browser 
