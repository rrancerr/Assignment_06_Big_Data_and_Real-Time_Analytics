E-Mail Generator
====

The e-mail generator takes random e-mails from the specified input directory and generates new e-mails in the specified
output directory using a Markov chain.

The binaries run out of the box. On UNIX-based systems (Linux, Mac) the executable run script is run.sh while on Windows
the executable run script is run.bat. Execute the respective script from the command line.

You may customize the parameters by editing the script. The command-line parameters i and o specify the input and
output directory, respectively. The command-line parameter f specifies the update frequency in seconds. For example,
an update frequency 10 means that new e-mails are written into the output directory every 10 seconds. The command-line
parameter s specifies how many e-mails are generated per interval. For example, a value 100 for s and a value 10 for i
means that 100 e-mails are generated every 10 seconds.

You may also copy additional e-mails into the input directory for more variety in your outputs.