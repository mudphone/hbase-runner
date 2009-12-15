# An HBase REPL

This is the beginnings of a tiny utility library for working with HBase in the
REPL.

Author: Kyle Oba ;; mudphone ;; koba <zat-yo> mudphone.com

# QUICK START:

1. clone the repo
2. execute this command from the repo root:
       $ ./hbase-repl.sh
3. connect with this command:
       user=> (start-hbase-repl)
4. do stuff, like list all tables:
       user=> (list-all-tables)
5. see below for comprehensive set-up instructions...
6. explore!


# PROJECT GOALS:
- Make this a comprehensive replacement for the HBase Shell.
- Allow non-lisp programmers to use this tool with ease.
- Allow ease of exploration of HBase data values and location.


# What's new:
- Now using HBase 0.20.2 for active development.

  * NOTE: You can use this with 0.20.0-1, you just have to replace the hbase
  and zk jars in lib/java with the proper versions.  It's that simple AFAIK.
  YMMV.

- Connections to HBase are now configured via the config/connections.clj file.
  This is basically a Clojure map with connection settings.  This means that
  hbase-site.xml is no longer required for hbase-runner to find your master.


# Some notes:
- The HBase jars must be on your classpath (including Zookeeper).
  They are currently included in lib/java and are put on the REPL classpath
  (if you use the included hbase-repl.sh script).
  If you do not use the hbase-repl.sh script, you have to add these jars to your
  classpath manually (or however you normally do it).

- For rlwrap goodness at the REPL (including tab-completion):
  1. make sure you have rlwrap installed (you can get it from macports if you're
     a mac drone).
  2. Also, set up your ~/.inputrc file (a sample is in config/.inputrc).
  3. Then, create a ~/.clj_completions file (you can run utils/completions.clj
     to create one).
     Run this:
         $> hbase-repl.sh utils/completions.clj
  4. If that doesn't work for you, you can copy the .clj_completions.sample file
     to ~/.clj_completions

- Truncating tables in a pmap is great because it's fast.  However, since the
  truncate calls are executed in agents, the output of the command is not seen.
  The truncate-tables command set up to return a list of maps, which are keyed
  with either :truncated or :error.  There is one map per truncated table.
  This return structure allows you to see if everything ran as expected.

- There's a test suite.  But, it's not comprehensive yet.  To run it, do this:

  1. Running tests from the command-line:
         $> ./run_tests.sh

  2. Running tests from the Emacs REPL:
     - Start slime.
     - open the spec file you want to run
     - eval buffer
           EMACS: M-x slime-eval-buffer (or C-z on my set-up)
     - enter the spec's namespace:
           user> (in-ns 'mudphone.hbase-runner-spec)
     - connect to the test DB:
           user> (start-hbase-repl :test "hbr_spec")
     - run all specs:
           user> (run-tests)
     - run a focused spec:
           user> (dump-table-test)

  3. Actually, the easiest way to run the tests is via swank-clojure-project
     and the clojure-test minor mode.  If you have those set up, you can just
     open a spec buffer and run C-c C-,


# KNOWN BUGS:
- At times, the truncate-tables function will not properly recreate a table.
  Be sure to save the result of your truncate-table(s) calls, like so:
    $> (def result (truncate-tables list-o-tables))
  "result" will now contain a helpful map of the tables and their statuses.
  If there was error, you can enable/disable, or use the included HTable
  descriptor to re-create the table.

  Use (:errors result) on the result of truncate-tables to find tables with
  errors.

  NOTE: This seems to be less of a problem with HBase 0.20.2.


# Instructions for use:

## Setup
If you are planning to run hbase-runner from Emacs you must set up the
HBASE_RUNNER_HOME environment variable.  Set this to where you cloned
this project.
    $> export HBASE_RUNNER_HOME=<some/path>
    $> cd $HBASE_RUNNER_HOME

Configure your HBase connection by copying config/connections.template.clj to
config/connections.clj and edit this file to reflect your HBase set-up.
    $> cd config
    $> cp connections.template.clj connections.clj
    $> emacs connections.clj

## Running the HBase-Runner REPL

### Via Script

To get a REPL, you can use the hbase-repl.sh script:

  Run the included repl script:
      $> ./hbase-repl.sh

  Note: Since hbase-repl.sh uses rlwrap, if you have it installed, you will get
  Clojure and HBase-Runner tab-completion!
    For example:
        > (truncate-t<tab> ;;=> (truncate-table
    See rlwrap note above to set up your list of completions.

  Users of the hbase-repl.sh script, do not have to explicity "use" the library
  because it is automatically loaded by the script.

### Via Emacs

If you're using Emacs, and you're working with Slime, you can include the
library like this:
    user=> (require 'mudphone.hbase-runner.hbase-repl)

And, you can throw yourself into the namespace if you want, like this:
    user=> (in-ns 'mudphone.hbase-runner.hbase-repl)


### In General

While in the REPL...

  Create a connection to HBase...
  If you want to work with all tables:
      user=> (start-hbase-repl)

  If you would rather provide a table namespace (prefix)
      user=> (start-hbase-repl "koba_development")
      ;; the following would force most commands to only work with tables
      ;; beginning with "koba_development_"

  This prints all HBase tables:
      user=> (list-all-tables)

  This prints all HBase tables in your "namespace".
      user=> (list-tables)

  Print system settings, such as table-ns, selected system, and
  HBASE_RUNNER_HOME:
      user=> (print-current-settings)

  To truncate tables in parallel:
      user=> (def result (truncate-tables list-o-tables))
      ;; where "list-o-talbes" is a list of tables that you've def-ed somewhere.

  If, you are in the hbase-repl namespace, you can use pretty print:
      user=> (pp) ;; to see the last result
      or
      user=> (pprint result)

  I hope to make it easier to find out why things go wrong while pmap-ing the
  table truncations.  For now, you can do this:
      user=> (def result (truncate-tables list-o-tables))
      user=> (:errors result)    ;; => to see tables with errors
      user=> (:truncated result) ;; => to see tables which were truncated

Truncated tables are keyed with :trunacted.  If there was an error on
truncation, they will be keyed with :error.  You will want to check the console
output if there were errors.  Also, try enable/disable on the tables, as this
is frequently a problem.

The result from a truncate-table(s) call contains the original HTable
descriptor. To retrieve a table which has been dropped in error, you can use
this descriptor with the create-table-from function.

To re-create the first table with an error (assuming it was dropped):
    user=> (def result (truncate-tables list-o-tables))
    ;; This is where things went wrong.
    ... output with errors ...
    user=> (create-table-from (:descriptor (first (:errors result))))


The current public API includes:

count-region [htable start-key end-key]
count-rows [table-name]
create-table-from [descriptor]
current-table-ns []
disable-table [table-name]
drop-table [table-name]
dump-tables
enable-table [table-name]
find-all-tables [search-str]
find-tables [search-str]
flush-table [table-name]
flush-table-or-region [table-name-or-region-name]
hydrate-table-maps-from [file-name]
list-all-tables []
list-tables []
major-compact [table-name-or-region-name]
print-current-settings []
set-current-table-ns [current-ns]
start-hbase-repl
table-disabled? [table-name]
table-enabled? [table-name]
table-exists? [table-name]
truncate-tables [table-name-list]


Enjoy!


# TODO (in rough order of precedence):
- Make this a comprehensive replacement for the HBase shell.
- Implement: "regions-for-table"
             "diable-drop-table-if-exists"
             "close_region"
- Complete test coverage of full public API.
- Remove hard-coded (ooops) output / input file paths for table dump files.


# Thanks go to:

- Amit Rathore (#amitrathore) - Much code was taken from his
                                clojure util libraries.
- Clojure Contrib Authors - I borrowed and stole much.

- For keeping our HBase up and getting us up to the required number of nodes:
  * Siva Jagadeesan (#sivajag)
  * Robert J. Berger (#rberger)

- HBase Team:
  For tons of help on IRC:
  * Michael Stack (#St^Ack)
  * Ryan Rawson (#dj_ryan)
  * J-D Cryans (#jdcryans)

  You can find me logged in as #mudphone.

- And, everyone else working on HBase.  Thanks!!!


                                       ,.:r,          
                                      ..   :          
                ,..,                  i    .r         
              ,RQQQQQRFr,   .jbQQQQQQX7.  ,,:7,,      
              :QQRRRQQQQQ9cQQQQQQQQQQQQQR7  i7r.::.., 
               MQb00bDRXr,  :pQQRb00bbEQQQQQ. i,  ,...
                RZpP92,        ibQR9Ppp9RQQQQi r,   i.
                 DZ1r i    L     ,fQQ0PXbZMRQQU r:ii. 
                  Ji  UQ   PQ       7RQ0pDEDRQQ7 .    
                 ,i  .cQ  .jQ         iMQREEDMQQ      
                iJ   SQQ  1QQ           :RQREDRQF     
               r2    rQQ  rQQ             :RQRZQQ     
              i1.     :    r                7QQQQ     
      .:irrrr7tJ::::.                         2QQ     
      rt..,,,,  ,.........,,             ,     iQ     
       7                                ,,,     U     
       .Y                               ,,,,,  .r     
        7i                              ,,,,,, ii     
         f,                             ,,,,,, 7.     
         :U                             ,,,,,, c,     
          ci                           ,,,,,, ,c      
           F,                          ,,,,,, :7      
           :t                          ,,,,,, ri      
            J:                         ,,,,,  c.      
            .Q1UJYL7ri:::..,,,                L       
            JQQQQQQQQQQQQQQQQQQQQQQQQQQRRZb9h09       
            QREEEDMMRQQRRRRRRQQQQQQQQQQQQQQQQQQ       
           :Q9XXXpp0Z 0R009900000bbb0DMZMMMMZRQ       
           XRXShSPXR   QDPXXXXXXXPPXXX0090090ZQ,      
           Q0XXSPpR.   ,QEpPPPPPXXXXXSP99ppp9DQ:      
          7QbbbbDRr     :QRZDE0pXXXXXXX99ppp9bQ7      
          Qi .....       ,.:iiihbXXXXXSp9ppp9bQ2      
         .Qt                  .P9XXXXXSP9ppp90Qb      
         XQZQ2              :MR0PXXXXXSX9ppp90RQ      
         QZbMQQL          iQQR0pPXXXXXXX99pp90MQ      
        rQPP0DQQS         QQM0PXXXXXXXXX99ppp9MQ,     
        RMSSX9RQ           QEpXXXXXXXXXX99ppp9DQ:     
       .QpShSPQ:    ,:     rQ9PXXXXXXXXX99ppp9EQ7     
       2QShhhbU   7DQQQf.   0DpXXXXXXXXX99ppp9bQt     
       QbhhhPD :bQQQQQQQQZ:  DpXXXXXXXSP99ppp90Q0     
      rQXShhEMRQQRDb0bDMQQQQifbXXXXXXXSP9pppp90RQ     
      MMShhX9bDE09PPPPp9bDMMZDbXXXXXXXSp9pppp90RQ     
      Q9ShSPPpPPXXXXXXXPpp99pXXXXXXXXXSp9pppp99MQ,    
     LQShhXPXXXXXXXXXXXXXXPPXXXXXXXXXXXp9ppppp9DQ:    
    ,REShSPXXXXXXXXXXXXXXXXXXXXXXXXXXXX99ppppp9EQr    
    JQZPhXPXXXXXXXXXXXXXXXXXXXXXXXXXXSP09ppppp9bQt    
    :iEQRD0PPXXXXXXXXXXXXXXXXXXXXXXXXXp9pppppp90Q9    
        r0QQRE9PPXXXXXXXXXXXXXXXXXXXSX99pppppp90RR    
           .cEQQRD09pPXXXXXXXXXXXXXXSp9pppp9990EQQ    
               .cXMQQRDEb009ppPPPPPPpb0bEDZMRRRQQQ,   
                     iPP0ZRRQRRRQQQRRRRRREhJr:,       
                      i      :   ,, .i                
                     .c      .       i                
                     :j      i.     ,J                
                     rj      ri    ,,U.               
                     7J      ir    , ci               
                     Lc      :7    , rL               
                     Y7      .c    , :U               
                     jr      .J      .2,              
                    ,fi      .J     ,,f.              
                     i.       :       :.

http://www.hrwiki.org/wiki/User:Soapergem/ASCII_Art