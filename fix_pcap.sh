#!/bin/bash
for i in $(ls ./result/origin)
do 
    pcapfix -d ./result/origin/$i -o ./result/fix/$i
done