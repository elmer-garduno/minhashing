metodos-minhashing
==================

MapReduce Locality Sensitive Hashing clustering.

This method is based on Broder '97 _Syntactic Clustering of the Web_.
Plus LSH as described on Rajaraman, Leskovec and Ullman 2012
and partially on code originally found on org.apache.mahout.clustering.minhash.MinHashMapper
available under the Apache License 2.0.


## Clustering Wikipedia articles from its categories

We will use the top 100K rows of dbpedia article-categories dataset to test. 

```
# Create the jar
mvn jar:jar
# Unzip the dataset data/wiki-100000.zip and put it into the dfs
hadoop dfs -put data/wiki-100000.txt wiki-100000.txt 
```

Concatenate the categories on each article and create a sequence file in the expected format <id, content> 
in our case <article, cat(categories)>

```
hadoop jar target/minhashing-1.0.0-SNAPSHOT.jar mx.itam.metodos.tools.HadoopGroupWikiCategories -libjars guava-13.0.1.jar wiki-100000.txt categories-seqfiles
```

Create k-shingles from each article categories, this step creates a file with the format <id, list(shingles)>

```
hadoop jar target/minhashing-1.0.0-SNAPSHOT.jar mx.itam.metodos.shingles.HadoopShingles categories-seqfiles categories-shingles 5
```

Cluster using the method described on section 3.4.3 of [Mining of Massive Datasets v1.2](http://infolab.stanford.edu/~ullman/mmds.html)

```
hadoop jar target/minhashing-1.0.0-SNAPSHOT.jar mx.itam.metodos.lshclustering.HadoopLSHClustering -libjars guava-13.0.1.jar categories-shingles categories-out 5 20
```

Verify the output clusters

```
hadoop dfs -libjars target/minhashing-1.0.0-SNAPSHOT.jar -text categories-out-5-20/part-00000 | more
```

## Full example with a larger dataset

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

Use the `article_categories_en.txt` as the input file and perform the same procedure described above.
