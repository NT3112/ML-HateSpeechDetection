from hate.configuration.gcloud_syncer import GCloudSync

obj=GCloudSync()
obj.sync_folder_from_gcloud("hate-speech31","dataset.zip","data/dataset.zip")