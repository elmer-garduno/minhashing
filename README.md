metodos-minhashing
==================

MapReduce Locality Sensitive Hashing using minhashes.

This method is based on Broder '97 _Syntactic Clustering of the Web_.
Plus LSH as described on Rajaraman, Leskovec and Ullman 2012
and partially on code originally found on org.apache.mahout.clustering.minhash.MinHashMapper
available under the Apache License 2.0.


# Example
_Clustering Wikipedia articles using their categories_

We will use the top 100000 rows of dbpedia's article categories dataset to test. 


```
# First unzip the dataset data/wiki-100000.zip and put it into the dfs
hadoop dfs -put data/wiki-100000.txt wiki-100000.txt 
```

The following step concatenats each of the article categories into the article 
and create a sequence file in the expected format <id, content> in this case <article, categories>

```
hadoop jar minhashing-hadoop.jar mx.itam.metodos.tools.HadoopGroupWikiCategories -libjars guava-13.0.1.jar wiki-10000.txt categories-seqfiles
```


# Full example with a larger dataset

Download the article categories form dbpedia

```
http://downloads.dbpedia.org/3.8/en/article_categories_en.nt.bz2
```

Unzip and pre-process to remove the n-triples markup, each row contains an article and a corresponding category.

```
cat article_categories_en.nt |perl -pe 's|<(.*?)>|\1|g'| \
awk '{printf "%s %s\n",$1,$3}' |perl -pe 's|http://dbpedia.org/resource/||g'| \
perl -pe 's| Category:| |g' > article_categories_en.txt
```

Concatenate each of the article categories and create a sequence file in the expected format <id, content> in this case <article, categories>

```
hadoop jar minhashing-hadoop.jar mx.itam.metodos.tools.HadoopGroupWikiCategories -libjars guava-13.0.1.jar article_categories_en.txt categories-seqfiles
```
