function pore_count {
	for i in $(find . -name "pore_scan_data*.csv");
	do 
		echo -e "$i\t$( \
		grep 'single_pore' $i | \
		cut -f 16,53 -d ',' | \
		sed 's/,/\t/g' | \
		sort -k2,2g | uniq -c | tail -1)"
	done
}
