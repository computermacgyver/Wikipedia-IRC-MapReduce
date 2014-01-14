Wikipedia-IRC-MapReduce
====================

Wikipedia-IRC-MapReduce

This Java code shows examples to parse and extract data from IRC streams of recent changes to Wikipedia chaptured with [Wikipedia-IRC-Logger](https://github.com/computermacgyver/Wikipedia-IRC-Logger) It was used to monitor the recent changes to Wikipedia in multiple language editions and the results published in [a recent article](http://arxiv.org/abs/1312.0976).

Requirements
The following must be available on the classpath to compile and run:
* Douglas Crockford's [JSON-java](https://github.com/douglascrockford/JSON-java)
* [Hadoop](http://hadoop.apache.org/)

The most useful file is [RecentChange.java](), which provides example code to parse lines from the files. 

AllEdits simply parses all the lines of all input files and creates one json object for each edit. The resulting file(s) have multiple json objects separated by a newline character.

UsersByLangEdition parses all lines of all input files and creates a tab separated values with one line per username that edited the encyclopedia. It replies on a file localAccounts that lists usernames that are not global accounts to separate these users. This script was first run with an empty localAccounts file and then that file was built by checking all users who edited multiple language editions against the Central Authorization database for  Wikipedia (see article for details). It's output is tab separted values that are username, number of edits, average edit size, most-edited language, edits in most edited language, number of languages edited.


If you use this code in support of an academic publication, please cite:
    Hale, S. A. (2014). Multilinguals and Wikipedia Editing. http://arxiv.org/abs/1312.0976
  

