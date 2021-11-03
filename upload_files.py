'''
upload any new local files
and save these local files with the gen3 object ids in a csv file
'''
import yaml
from oeps_utils.file_utils import Files

#upload OEPS files and save file propreties to a file for metadata submissions etc
config = yaml.safe_load(open('config.yaml','r'))
files = file_utils.Files(**config['file_params'])
#files.upload_all_files_with_subdirs(**config['file_upload_params'])
files.upload_new_files(**config['file_upload_params'])
#get local files with object ids (should all have object ids if uploads were successful)
files_df = files.merge_local_and_gen3_file_info()
files_df.to_csv(config['csv_file_save_path'])


# auth = Gen3Auth(refresh_file='credentials.json')
# sub = Gen3Submission(auth)
# index = Gen3Index(auth)

# #attempt to get files with subdirectory in name - but ran out of time
# df = get_gen3_files(index)
# df['len_file_name'] = df.file_name.apply(lambda x: len(x) if x else 0)
# has_subdir = df.file_name.str.contains("metadata|moud|geometryFiles|\.csv")
# df.sort_values(['md5sum','len_file_name']).groupby(['md5sum','file_size']).head(1).to_csv('file_subdirs.csv')


# sub.delete_nodes("JCOIN", "TEST", ['reference_file','core_metadata_collection'])
# sub.delete_nodes("JCOIN", "OEPS", ['reference_file','core_metadata_collection'])
