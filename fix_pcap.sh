#!/bin/bash
for i in $(ls ./result/pcap_record)
do 
    pcapfix -d ./result/pcap_record/$i
done