# ANA: AWS-Ngrams-Analyzer
`ANA` is an analyzer of ngrams. It gets the public [Google Ngrams from S3](https://aws.amazon.com/datasets/google-books-ngrams/) and executes a few hive scripts on EMR to find neologisms and foreignisms in some languages.

## What do I need?
- An AWS account with access to EMR (Usually the free tier option does **not** include EMR).
- [AWS access keys](http://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html).
- Java


## Quick start
If you fulfilled the previous requirements then you are ready to run the analyzer, this is what you should do:
- Ask the gradle wrapper to build the project:
```
./gradlew build
```
- You will now see a `build` folder, run the jar in its `libs` folder:
```
java -jar ./build/libs/aws-ngrams-analyzer.jar
```

Now, just select the options that `ANA` offers to you to run the analyzer, and then visit your new S3 bucket to find the logs and results. You can keep on the process through the [aws console](https://console.aws.amazon.com), just check out S3 and EMR (which runs on top of EC2).


## How it works
`ANA` creates a bucket in S3, uploads the hive scripts and generates EMR steps to run them. They generate up to 20 neologisms by year and 1K foreignisms on the language specified.

### Finding neologisms
For the selected main language, `ANA` creates a window of size **W** and selects all of the ngrams in that period. Then, it shifts the window year by year, and at every shift it selects the ngrams that didn't occur at least **percentOfYears** in the previous window (i.e.: 80% in a window of size 5 it would be 4), but after the shift they do (in the previous example, the ngram was 3 out of 5 years in the window but after the shifting the counters get updated and they reflect 4 out of 5 years now, so we found a neologism).

### Finding foreignisms
A foreignism is a foreign term that one language borrowed from another one. `ANA` selects all the ngrams of the second language selected, and looks for matches in both languages trying to detect when some ngram occurs in both of them but with a big usage ratio difference. Bad labeling in some books of the corpora and quotes from different languages may cause the output to be filled with the most common words of one language. But with better data it works just fine. I leave it as a PoC.


## Problems
- Many OCR errors (particularly before the 19th century).
- Data with incorrect language label.
- Empty years on ancient times.

The safest bet to avoid these problems is to select a range of years within the 20th and 21st centuries.

## License
```text
Copyright 2016 Julian De Luca.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
