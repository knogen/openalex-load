

# Road Of OpenAlex
[TOC]

## Perface

[openalex](https://docs.openalex.org/) 继承了 MAG 的数据，也进行了一些较大的[改动](https://docs.openalex.org/download-snapshot/mag-format/mag-migration-guide), 去除 Patent 等类型的 Paper，使用 [Concepts](https://docs.openalex.org/about-the-data/concept) 取代了 MAG 的 field of study 对概念的标注。

数据有了跟多维度的开放，能够找到作者和机构的位置信息，数据中也包含了 Paper 的摘要信息。数据的更新频率更快，几乎每天都有新数据的加入和旧数据的更改。

## Data Processing

openalex 提供在线 api 和 snapshot 两种方式获取数据，我们使用 snapshot 类型的数据，从托管在 [amazonaws](https://openalex.s3.amazonaws.com/browse.html)的服务中，下载完整的数据，全都数据打包后超过 300GB。

### Get OpenAlex

从 [download-snapshot](https://docs.openalex.org/download-snapshot/download-to-your-machine) 中找到数据的下载和更新方式。安装好 `aws` 后通过:

`/usr/bin/aws s3 sync "s3://openalex" "/home/ni/data/openalex-snapshot" --no-sign-request --delete`

将 snapshot 同步到我们的本地。

### Prepare data

snapshot 数据的更新是删除，增加，修改压缩包，并且承诺一个 snapshot 中不存在相同 ID 的两条数据。我们可以放心的将数据 insert 到空的数据池而不用担心冲突，但是如果要找到 snapshot 做了哪些改动就是麻烦的事。

根据上面的特性，我们尽可能不进行增量更新数据到本地的数据池，每次使用完整的 snapshot 文件处理，直接产生数据结果。经过已有的实践，多任务的完整读取超过 200GB 的 **works** 数据，将其处理成我们需要的格式并暂存到内存中，整个过程也只需要 *1 小时* 左右的时间，这个速度超过我们单线程从数据库池加载中间数据的速度。

### 

