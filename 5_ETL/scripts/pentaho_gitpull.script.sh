function pull_datasets {

	if [[ ! -d "$DATA_DIR/$1" ]]
	then	
	    cd $DATA_DIR
	    git clone  $2 $1
	fi
	cd $DATA_DIR/$1; git pull

}

# set download path 
export DATA_DIR='/Users/alexw/Desktop/Covid-Data-Warehousing-ETL/2_Datasets'


export REPO_DIR='nCoV-nytimes'
pull_datasets $REPO_DIR   https://github.com/nytimes/covid-19-data.git

export REPO_DIR='nCoV-beoutbreakprepared'
pull_datasets $REPO_DIR https://github.com/beoutbreakprepared/nCoV2019.git


