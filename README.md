# Ops terra utils

## Development / Testing


If this is your first time using this image or if you've made changes to the base image recently: 
> docker compose up --build ops-toolbox

Running this container interactively:
> docker compose run ops-toolbox bash


## Adding WDLs to dockstore
To add new WDLs to dockstore you need to add the WDL information to the `.dockstore.yml` file. You can follow the previous examples in the file to add new WDLs.