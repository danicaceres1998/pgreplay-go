mkdir ./sorted
cd sorted

# Sort each file
for f in $(ls "../$PGREPLAY_PID"); do
    echo "Sorting File -> $f"
    sort -t ',' -k 1 "../$PGREPLAY_PID/$f" > $f
done

# Merge all files
echo "merging $(ls .)"
sort -bsm -t ',' -k 1 $(ls .) > ../combined.csv

# Remove other files
cd ..
rm -rf sorted
rm -rf "$PGREPLAY_PID/"
