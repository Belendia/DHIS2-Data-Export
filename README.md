# DHIS2 Data Export

## Setup your development environment
- Install the latest version of Python3 on your computer.
- Navigate into your home directory
    ```
    cd~
    ```
- Create .virtualenvs directory in at your home directory 
  ```
  mkdir .virtualenvs
  ```
- Install virtualenv 
  ```
  pip install virtualenv
  ``` 
- ```which virtualenv```
- Install virtualenvwrapper 
  ```
  pip install virtualenvwrapper
  ```
- After running the commands above, open the .bash_profile file via vim or any other tool you prefer.
    ```
    vim .bash_profile
    ```
- Press shift+I to add the next two lines to your .bash_profile and paste the following lines.
    ```
    export WORKON_HOME=~/.virtualenvs
    source /usr/local/bin/virtualenvwrapper.sh
    ```
- To save and exit vim using ```:wq``` command.

## How to run the program
- Create virtual environment
    ```
    mkvirtualenv dhis
    ```
- Install the dependency libraries
    ```
    pip install -r requirements.txt
    ```
- Open app.py and change SERVER, USERNAME and PASSWORD
- Run the program
  ```
  python app.py
  ```

