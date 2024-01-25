source_dir="/data/RUSHAD_P12"
#dest_dir="$scg:/oak/stanford/groups/smontgom/tannerj/RUSH_AD/promethion_data"
dest_dir="$scg:/oak/stanford/groups/smontgom/tannerj/RUSH_AD/promethion_data/RUSHAD_P12"

# find every files that hasn't been modified in past 30 min (ensures files are quiescent)
# transfers them to destination and deletes them on source
find $source_dir -cmin +5 -printf %P\\0 \
    | rsync -rlvtW --files-from=- --from0 --remove-source-files $source_dir $dest_dir

#rsync -rlvtW $source_dir $dest_dir


