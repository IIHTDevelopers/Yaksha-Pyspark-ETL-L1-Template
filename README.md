### Scenario
`etl_job.py` processes ~5 GB CSV input and crashes with **OutOfMemoryError: Java heap space**.

### Objective
Refactor the job to avoid driver OOM **without changing cluster size**. Hint: remove `collect()` and keep transformations on the cluster.

### Running the testcase 
use the code python -m unittest to run the testcase 

### submit the code the git 

Make sure before final submission you commit all changes to git. For that open the project folder available on desktop
1)Right click in folder and open Git Bash
2)In Git bash terminal, run following commands
3)git status
4)git add .
5)git commit -m “First commit”
(You can provide any message every time you commit)
6)git push
then click on submit assessment button


