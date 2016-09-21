/**
  * This project is about computing subgraph matching with spark in distributed environment.
  *
  * The work's idea comes from following paper:
  * Sun, Zhao, et al. "Efficient subgraph matching on billion node graphs." Proceedings of the VLDB Endowment 5.9 (2012): 788-799.
  *
  * Developed by Tang Lizhe, National Laboratory for Parallel and Distributed Processing, National University of Defense Technology, Changsha, China;
  */


package subgraph

import breeze.linalg.{SparseVector => BSV}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}


/**
  * Created by Tang Lizhe on 2015/11/12.
  * All rights preserved.
  */

object Main {
  def main(args: Array[String]) {
    val numPartitions = 1
    val checkPointInternal = 4

    if (args.length != 6) {
      println("Usage: <DataGraphVertexFilePath> <DataGraphEdgeFilePath> <QueryGraphVertexFilePath> " +
        "<QueryGraphEdgeFilePath> <ResultOutputFilePath> <TempWorkDirectoryPath>")
      System.exit(0)
    }

    val dataGraphVertexFilePath = args(0)
    val dataGraphEdgeFilePath = args(1)
    val queryGraphVertexFilePath = args(2)
    val queryGraphEdgeFilePath = args(3)
    val resultOutputFilePath = args(4)
    val tempWorkDirectoryPath = args(5)

    val conf = new SparkConf()
      .setAppName("NUDT Subgraph Matching")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir(tempWorkDirectoryPath)

    val dataGraphVtx = sc.textFile(dataGraphVertexFilePath).filter(_.length > 0).map { x =>
      val s = x.split(" ")
      (s(0).toLong, s(1).toInt)
    }

    val dataGraphEdgeTuple = sc.textFile(dataGraphEdgeFilePath).filter(_.length > 0).map { x =>
      val s = x.split(" ")
      (s(0).toLong, s(1).toLong)
    }

    // mix vertex and tag to long type, create combined data graph edge
    val cmbDataGraphEdge = makeCmbEdge(dataGraphVtx, dataGraphEdgeTuple, numPartitions).cache()

    val queryGraphVtx = sc.textFile(queryGraphVertexFilePath).filter(_.length > 0).map { x =>
      val s = x.split(" ")
      (s(0).toLong, s(1).toInt)
    }.cache()

    val queryGraphEdgeTuple = sc.textFile(queryGraphEdgeFilePath).filter(_.length > 0).map { x =>
      val s = x.split(" ")
      (s(0).toLong, s(1).toLong)
    }.cache()

    // map query graph's vertex to index of array
    val vtx2Idx = queryGraphVtx.map(_._1).collect().zipWithIndex.toMap
    val vtx2IdxBc = sc.broadcast(vtx2Idx)
    val vtx2Tag = queryGraphVtx.collect().toMap
    val vtx2TagBc = sc.broadcast(vtx2Tag)

    // generate tags swigs and vertex swigs for query graph
    val (swigs, swigsVtx) = generateSwigs(queryGraphVtx, queryGraphEdgeTuple)
    val swigsBc = sc.broadcast(swigs)
    val swigsVtxBc = sc.broadcast(swigsVtx)

    queryGraphVtx.unpersist()
    queryGraphEdgeTuple.unpersist()

    // processing subgraph matching for each swig
    val start = System.currentTimeMillis()
    val matchRst = cmbDataGraphEdge.mapPartitions(swigsMatchOnPartition(swigsBc)).zipWithUniqueId().cache()

    // collect matching result for each swig along all partitions
    val matchRDDs = new Array[RDD[(Long, Array[Array[Long]])]](swigs.length)
    val matchRstPartitionSize = matchRst.partitions.length
    for (i <- swigs.indices) {
      matchRDDs(i) = matchRst.filter(_._2 / matchRstPartitionSize == i).map(_._1).flatMap(_.toIterator)
    }
    val swigsMatchRst = matchRDDs.map(_.collect())
    val swigsMatchRstBc = sc.broadcast(swigsMatchRst)
    val end = System.currentTimeMillis()

    matchRst.unpersist()
    cmbDataGraphEdge.unpersist()

    // generate join order
    val swigsMatchCount = swigsMatchRst.map(_.length)
    val joinOrder = generateSwigJoinOrder(swigs, swigsVtx, swigsMatchCount)

    // join swig's matching result in parallel
    val start2 = System.currentTimeMillis()
    var partJoinedGraph = sc.parallelize(Array[Array[Long]](), numPartitions)
    val joinPartitionSize = partJoinedGraph.partitions.length

    val joinedVtx = new mutable.HashSet[Long]() // record the joined vertex for query graph
    var internal = checkPointInternal
    var firstRun = true
    for (i <- joinOrder) {
      if (firstRun) {
        // join first swig matching result
        partJoinedGraph = partJoinedGraph.mapPartitionsWithIndex(firstSwigJoin(i, joinPartitionSize)
        (swigsVtxBc, vtx2IdxBc, vtx2TagBc, swigsMatchRstBc))
        firstRun = false
      }
      else {
        // join rest swig matching result
        partJoinedGraph = partJoinedGraph.mapPartitions(nextSwigJoin3Layer(i, joinedVtx.toSet)
        (swigsVtxBc, vtx2IdxBc, vtx2TagBc, swigsMatchRstBc))
      }

      // make a checkpoint to avoid long time recomputing if fail
      internal -= 1
      if (internal == 0) {
        internal = checkPointInternal
        partJoinedGraph = partJoinedGraph.repartition(numPartitions)
        partJoinedGraph.checkpoint()
      }

      joinedVtx += swigsVtx(i)._1
      joinedVtx ++= swigsVtx(i)._2
    }

    // output subgraph matching result
    val subGraphOut = partJoinedGraph.map(_.map(x => getId(x)).mkString(",")).cache()
    val resultCount = subGraphOut.count()
    subGraphOut.saveAsTextFile(resultOutputFilePath)
    val end2 = System.currentTimeMillis()

    val summary =
      "--------------------------Summary--------------------------\n" +
        s"swig number:${swigs.length}\n" +
        s"swig structure\n" +
        s"${swigsVtx.map(x => x._1.toString + ": " + x._2.mkString(",")).mkString("\n")}\n" +
        s"swig matched size\n" +
        s"${swigsMatchCount.mkString("\n")}\n" +
        s"match time: ${(end - start) / 1000.0}s\n" +
        s"join time: ${(end2 - start2) / 1000.0}s\n" +
        s"join order: ${joinOrder.mkString(",")}\n" +
        s"result count: $resultCount \n" +
        s"-----------------------------------------------------------\n"
    println(summary)

    sc.stop()
  }


  /**
    * generate vertex swig and tag swig for query graph
    *
    * @param vertex    query graph vertex
    * @param edgeTuple query graph edge
    * @return array for vertex swig and tag swig
    */
  def generateSwigs(vertex: RDD[(Long, Int)], edgeTuple: RDD[(Long, Long)])
  : (Array[(Int, Array[Int])], Array[(Long, Array[Long])]) = {
    val edge = edgeTuple.collect()
    val vtxTag = vertex.collect()
    val numVtx = vtxTag.length
    val idx2Vtx = vtxTag.map(_._1)
    val idx2Tag = vtxTag.map(_._2)
    val vtx2Idx = idx2Vtx.zipWithIndex.toMap

    // collect swigs, the content is about tag
    val swigsBuf = new ArrayBuffer[(Int, Array[Int])]()
    // collect swigs, the content is about vertex
    val swigsVtxBuf = new ArrayBuffer[(Long, Array[Long])]()

    val matrix = Array.fill(numVtx)(BSV[Int](numVtx)()) // numVtx * numVtx
    edge.foreach(x => matrix(vtx2Idx(x._1))(vtx2Idx(x._2)) = 1)
    // neighbor vertex of each vertex, Array[Array[(vtx, tag)]]
    val nbVtx = matrix.map(_.activeKeysIterator.map(idx => (idx2Vtx(idx), idx2Tag(idx))).toArray)

    // vertex and it's neighbors
    var cmbVtx = vtxTag.zip(nbVtx).filter(_._2.length > 0) // Array[((Long, Int), Array[(Long, Int)])]
    while (cmbVtx.nonEmpty) {
      val swig = cmbVtx.head
      val rootId = swig._1._1
      val rootTag = swig._1._2
      val chdIds = swig._2.map(_._1)
      val chdTags = swig._2.map(_._2)

      swigsBuf += rootTag -> chdTags
      swigsVtxBuf += rootId -> chdIds

      cmbVtx = cmbVtx.tail
    }
    (swigsBuf.toArray, swigsVtxBuf.toArray)
  }

  /**
    * generate join order
    *
    * @param swig           tag swig
    * @param swigVtx        vertex swig
    * @param swigMatchCount count of each swig matching result
    * @return the order for swig matching result join
    */
  def generateSwigJoinOrder(swig: Array[(Int, Array[Int])], swigVtx: Array[(Long, Array[Long])]
                            , swigMatchCount: Array[Int]): Array[Int] = {
    val joinedVtx = new mutable.HashSet[Long]()
    val order = new Array[Int](swig.length)
    val outDgrMatchCount = swig.map(_._2.length).zip(swigMatchCount)
    var cmb = swigVtx.zip(outDgrMatchCount).zipWithIndex.map(x => (x._1._1, x._1._2, x._2))

    for (i <- swig.indices) {
      val cmbWithWeight = cmb.map { x =>
        val (root, chds, outDgr, matchCount, swigIdx) = (x._1._1, x._1._2, x._2._1, x._2._2, x._3)

        // has joined and not has joined count for this swig
        val rootJoinFlag = joinedVtx.contains(root)
        val chdJoinCount = chds.count(x => joinedVtx.contains(x))
        val chdUnJoinedCount = chds.length - chdJoinCount

        // the minimal weight swig will join priority
        val weight = if (joinedVtx.isEmpty) {
          matchCount.toDouble / outDgr / outDgr
        } else if (rootJoinFlag) {
          if (chdUnJoinedCount == 0) 0D
          else 1D
        } else {
          if (chdUnJoinedCount == 0) 2D
          else 3D
        }
        (root, chds, weight, swigIdx)
      }

      val root = cmbWithWeight.min(cmbVtxOrdering)
      order(i) = root._4
      joinedVtx += root._1
      joinedVtx ++= root._2

      cmb = cmb.filter(x => x._1._1 != root._1) // remove the swig root
    }

    order
  }

  /**
    * build index in edge partition and processing subgraph matching for each swig
    *
    * @param swigsBc tag swig
    * @param it      iteration of partition elems
    * @return matching result for each swig in a patition
    */
  def swigsMatchOnPartition(swigsBc: Broadcast[Array[(Int, Array[Int])]])(it: Iterator[(Long, Long)]):
  Iterator[Array[(Long, Array[Array[Long]])]] = {
    val swigs = swigsBc.value

    // sort edge by source vertex's id, dest vertex's tag
    val edges = it.toArray.sortWith { (a, b) =>
      if (getId(a._1) == getId(b._1)) // src id
        a._2 < b._2
      else
        getId(a._1) < getId(b._1) // dest tag
    }

    // map tag to source vertices which possess this tag in partition
    //val tag2Src = edges.map(_._1).groupBy(getTag).mapValues(_.toSet)
    val tag2Src = new mutable.HashMap[Int, mutable.HashSet[Long]]()
    edges.foreach { edge =>
      val tag = getTag(edge._1)
      if (!tag2Src.contains(tag))
        tag2Src += tag -> new mutable.HashSet[Long]()
      tag2Src(tag) += edge._1
    }

    // map (src, dstTag) to the index which indicates the fist satisfied edge in partition
    val srcDstTag2Idx = new mutable.HashMap[(Long, Int), Int]()
    var srcFlag = -1L
    var dstTagFlag = -1
    for (i <- edges.indices) {
      val (src, dst) = edges(i)
      val dstTag = getTag(dst)
      if (src != srcFlag || dstTag != dstTagFlag) {
        srcFlag = src
        dstTagFlag = dstTag
        srcDstTag2Idx += (src, dstTag) -> i
      }
    }

    // start processing subgraph matching
    val partitionMatchRst = new Array[Array[(Long, Array[Array[Long]])]](swigs.length)

    // for each swig
    for (i <- swigs.indices) {
      val swig = swigs(i)
      val (rootTag, childrenTags) = swig
      val chdTag2Count = mutable.HashMap.empty[Int, Int]
      childrenTags.foreach(tag => chdTag2Count += tag -> (chdTag2Count.getOrElse(tag, 0) + 1))
      val childrenTag = chdTag2Count.keys.toArray.sorted // accordance to nextSwigJoin defined sequence

      val rootMatchSet = tag2Src.getOrElse(rootTag, new immutable.HashSet[Long]())
      val rootMatchRst = new ArrayBuffer[(Long, Array[Array[Long]])]()

      // for each root
      for (root <- rootMatchSet) {
        val tagMatchRst = new ArrayBuffer[Array[Long]]()
        var allTagMatched = true // flag indicate whether all children satisfy matching

        // for each swig tag of root
        for (chdTag <- childrenTag if allTagMatched) {
          val firstIdx = srcDstTag2Idx.get((root, chdTag))
          firstIdx match {
            case Some(idx) =>
              val thisTagMatchRst = new ArrayBuffer[Long]()
              var idxTravel = idx
              while (idxTravel < edges.length && edges(idxTravel)._1 == root
                && getTag(edges(idxTravel)._2) == chdTag) {
                thisTagMatchRst += edges(idxTravel)._2
                idxTravel += 1
              }
              // tag matched result's count should equal or more than tag's vertex count
              if (thisTagMatchRst.length >= chdTag2Count(chdTag))
                tagMatchRst += thisTagMatchRst.toArray
              else
                allTagMatched = false

            case None => allTagMatched = false
          }
        }

        // swig's root and all child tag has been matched, we think matching successful
        if (allTagMatched)
          rootMatchRst += root -> tagMatchRst.toArray // append each root Matching result
      }

      partitionMatchRst(i) = rootMatchRst.toArray // add each swig matching result
    }

    partitionMatchRst.toIterator // return each swig matching result
  }

  /**
    * processing join between one partial subgraph matching elem and one swig matching result
    *
    * @param partialJoinedRst one elem of partial joined subgraph
    * @param vtx2Idx          query graph vertex 2 index
    * @param unJoinedMatchChd un match result
    * @param unJoinedChd      un match child
    * @return
    */
  def joinProcess(partialJoinedRst: Array[Long], vtx2Idx: Map[Long, Int])
                 (unJoinedMatchChd: Array[Array[Long]], unJoinedChd: Array[Array[Long]])
  : Array[Array[Long]] = {
    val unJoinedPers = new Array[Array[Array[Long]]](unJoinedMatchChd.length)
    for (i <- unJoinedPers.indices) {
      val m = unJoinedMatchChd(i).length
      val n = unJoinedChd(i).length
      val permutation = new Permutation(unJoinedMatchChd(i), m, n)
      val per = ArrayBuffer[Array[Long]]()
      while (permutation.hasNext)
        per += permutation.next()
      unJoinedPers(i) = per.toArray
    }

    /**
      * tag 1: Array(per 1, per2, per3, ...)
      * tag 2: Array(per 1, per2, per3, ...)
      * tag 3: Array(per 1, per2, per3, ...)
      *
      * productCount:(1,3,9),the eventual product is size(tag 1)*size(tag 2)*size(tag 3) = 27
      */
    val productCount = new Array[Int](unJoinedPers.length)
    var product = 1
    for (i <- unJoinedPers.indices) {
      productCount(i) = product
      product *= unJoinedPers(i).length
    }

    val oneRst = Array.fill(product)(partialJoinedRst.clone())
    for (j <- productCount.indices) {
      val gap = productCount(j)
      val size = unJoinedPers(j).length
      val chdIdx = unJoinedChd(j).map(vtx2Idx) // jth tag, idx in partial subgraph for unJoined chd

      for (i <- 0 until product) {
        val per = unJoinedPers(j)(i / gap % size) // jth tag, idx in tag for i
        chdIdx.indices.foreach(idx => oneRst(i)(chdIdx(idx)) = per(idx))
      }
    }

    //    for (i <- 0 until product) {
    //      for (j <- productCount.indices) {
    //        val gap = productCount(j)
    //        val size = unJoinedPers(j).length
    //
    //        val per = unJoinedPers(j)(i / gap % size) // jth tag, idx in tag for i
    //        val chdIdx = unJoinedChd(j).map(vtx2Idx) // jth tag, idx in partial subgraph for unJoined chd
    //        chdIdx.indices.foreach(x => oneRst(i)(chdIdx(x)) = per(x))
    //      }
    //    }

    oneRst
  }

  /**
    * join first swig matching result
    *
    * @param swigIdx         index of swig
    * @param numPartitions   parallel size for join processing
    * @param swigsVtxBc      vertex swig
    * @param vtx2IdxBc       vertex 2 index for query graph
    * @param vtx2TagBc       vertex 2 tag for query graph
    * @param swigsMatchRstBc swig matching result
    * @param pid             partition id
    * @param it              non use
    * @return the partial joined subgraph
    */
  def firstSwigJoin(swigIdx: Int, numPartitions: Int)
                   (swigsVtxBc: Broadcast[Array[(Long, Array[Long])]], vtx2IdxBc: Broadcast[Map[Long, Int]],
                    vtx2TagBc: Broadcast[Map[Long, Int]], swigsMatchRstBc: Broadcast[Array[Array[(Long, Array[Array[Long]])]]])
                   (pid: Int, it: Iterator[Array[Long]]): Iterator[Array[Long]] = {

    val (root, children) = swigsVtxBc.value.apply(swigIdx)
    val vtx2Idx = vtx2IdxBc.value
    val vtx2Tag = vtx2TagBc.value
    val tag2Chd = children.groupBy(vtx2Tag)
    val swigMatch = swigsMatchRstBc.value.apply(swigIdx)

    val partitionRst = new ArrayBuffer[Array[Long]]()
    for (i <- swigMatch.indices if i % numPartitions == pid) {
      val (rootMatch, chdTagMatch) = swigMatch(i)
      // val tag2MatchChd = chdTagMatch.map(x => (getTag(x.head), x)).toMap // swig chd tag mapping to it's matching vertex
      val tag2MatchChd = new mutable.HashMap[Int, Array[Long]]()
      chdTagMatch.foreach(x => tag2MatchChd += getTag(x.head) -> x)

      val unJoinedTag = tag2Chd.keys.toArray // tag
      val unJoinedChd = tag2Chd.values.map(_.sorted).toArray // tag -> chds
      val unJoinedMatchChd = unJoinedTag.map(tag2MatchChd).map(_.sorted) // tag -> matching result

      val partialJoinedRst = new Array[Long](vtx2Idx.size)
      partialJoinedRst(vtx2Idx(root)) = rootMatch
      val oneRst = joinProcess(partialJoinedRst, vtx2Idx)(unJoinedMatchChd, unJoinedChd)
      partitionRst ++= oneRst
    }

    partitionRst.toIterator
  }

  /**
    * join the rest swig match result with partial joined subgraph
    *
    * @param swigIdx         index of swig
    * @param vtxJoined       parallel size for join processing
    * @param swigsVtxBc      vertex swig
    * @param vtx2IdxBc       vertex 2 index for query graph
    * @param vtx2TagBc       vertex 2 tag for query graph
    * @param swigsMatchRstBc swig matching result
    * @param it              iteration of the partial joined subgraph
    * @return the partial joined subgraph
    */
  def nextSwigJoin(swigIdx: Int, vtxJoined: Set[Long])
                  (swigsVtxBc: Broadcast[Array[(Long, Array[Long])]], vtx2IdxBc: Broadcast[Map[Long, Int]],
                   vtx2TagBc: Broadcast[Map[Long, Int]], swigsMatchRstBc: Broadcast[Array[Array[(Long, Array[Array[Long]])]]])
                  (it: Iterator[Array[Long]]): Iterator[Array[Long]] = {

    val (root, children) = swigsVtxBc.value.apply(swigIdx)
    val vtx2Idx = vtx2IdxBc.value
    val vtx2Tag = vtx2TagBc.value
    val rootIdx = vtx2Idx(root)
    val swigMatchRst = swigsMatchRstBc.value.apply(swigIdx)

    val partitionRst = new ArrayBuffer[Array[Long]]()
    for (partialJoinedElem <- it) {
      val partialJoinedElemSet = partialJoinedElem.toSet
      for (swigMatch <- swigMatchRst) {
        val (rootMatch, chdTagMatch) = swigMatch
        val allChdMatch = chdTagMatch.flatMap(_.toIterator)

        val rootFlag = !vtxJoined.contains(root) && !partialJoinedElemSet.contains(rootMatch) ||
          rootMatch == partialJoinedElem(rootIdx) //avoid unexpected cycle
        if (rootFlag) {
          val chdFilter = new ArrayBuffer[Long]()
          val matchFilter = new mutable.HashSet[Long]() ++= allChdMatch
          var childFlag1 = true // if swig chd has joined, this chd match result must contain the value
          for (chd <- children if childFlag1) {
            if (vtxJoined.contains(chd)) {
              // has encounter this child, must ensure one vertex same as partialJoinedElem[chd's index]
              val lastRstChd = partialJoinedElem(vtx2Idx(chd))
              if (matchFilter.contains(lastRstChd))
                matchFilter -= lastRstChd // remove this matched child because is has be fixed in res
              else
                childFlag1 = false
            } else {
              chdFilter += chd // add child to unJoined children
              matchFilter --= partialJoinedElemSet //avoid unexpected cycle
            }
          }
          if (childFlag1) {
            if (chdFilter.isEmpty) {
              // all chd have joined, no need to modify lastRst
              val lastRst = partialJoinedElem.clone()
              lastRst(rootIdx) = rootMatch
              partitionRst += lastRst
            }
            else {
              val tag2ChdFilter = chdFilter.toArray.groupBy(vtx2Tag) // tag 2 children whose tag is the same
              val tag2MatchFilter = matchFilter.toArray.groupBy(getTag)

              val unJoinedTag = tag2ChdFilter.keys.toArray // swig tag size
              val unJoinedChd = tag2ChdFilter.values.map(_.sorted).toArray // match children for each tag
              val unJoinedMatchChd = unJoinedTag.map(tag2MatchFilter).map(_.sorted)

              val childFlag2 = unJoinedMatchChd.indices.forall(i => unJoinedMatchChd(i).length >= unJoinedChd(i).length)
              if (childFlag2) {
                val lastRst = partialJoinedElem.clone()
                lastRst(rootIdx) = rootMatch
                val oneRst = joinProcess(lastRst, vtx2Idx)(unJoinedMatchChd, unJoinedChd)
                partitionRst ++= oneRst
              }
            }
          }
        }
      }
    }
    partitionRst.toIterator
  }

  /**
    * join the rest swig match result with partial joined subgraph
    *
    * @param swigIdx         index of swig
    * @param vtxJoined       parallel size for join processing
    * @param swigsVtxBc      vertex swig
    * @param vtx2IdxBc       vertex 2 index for query graph
    * @param vtx2TagBc       vertex 2 tag for query graph
    * @param swigsMatchRstBc swig matching result
    * @param it              iteration of the partial joined subgraph
    * @return the partial joined subgraph
    */
  def nextSwigJoin3Layer(swigIdx: Int, vtxJoined: Set[Long])
                        (swigsVtxBc: Broadcast[Array[(Long, Array[Long])]], vtx2IdxBc: Broadcast[Map[Long, Int]],
                         vtx2TagBc: Broadcast[Map[Long, Int]], swigsMatchRstBc: Broadcast[Array[Array[(Long, Array[Array[Long]])]]])
                        (it: Iterator[Array[Long]]): Iterator[Array[Long]] = {

    val (root, children) = swigsVtxBc.value.apply(swigIdx)
    val vtx2Idx = vtx2IdxBc.value
    val vtx2Tag = vtx2TagBc.value
    val rootIdx = vtx2Idx(root)
    val swigMatchRst = swigsMatchRstBc.value.apply(swigIdx)

    val tag2Chd = children.groupBy(vtx2Tag)
    val tags = tag2Chd.keys.toArray.sorted // accordance to swigsMatchOnPartition defined sequence

    val partitionRst = new ArrayBuffer[Array[Long]]()
    for (partialJoinedElem <- it) {
      val partialJoinedElemSet = partialJoinedElem.toSet
      for (swigMatch <- swigMatchRst) {
        val (rootMatch, chdTagMatch) = swigMatch

        val rootFlag = !vtxJoined.contains(root) && !partialJoinedElemSet.contains(rootMatch) ||
          rootMatch == partialJoinedElem(rootIdx) // avoid unexpected cycle in results
        if (rootFlag) {
          val tag2UnJoinedChd = new mutable.HashMap[Int, mutable.HashSet[Long]]()
          val tag2UnJoinedMatchChd = new mutable.HashMap[Int, mutable.HashSet[Long]]()
          tags.foreach(tag => tag2UnJoinedChd += tag -> new mutable.HashSet[Long]())
          tags.indices.foreach(i => tag2UnJoinedMatchChd += tags(i) -> (new mutable.HashSet[Long]() ++= chdTagMatch(i)))

          var tagFlag = true
          for (tag <- tags if tagFlag) {
            val chds = tag2Chd(tag)
            val chdUnJoined = tag2UnJoinedChd(tag)
            val matchChdUnJoined = tag2UnJoinedMatchChd(tag)

            var chdFlag = true
            for (chd <- chds if chdFlag) {
              if (vtxJoined.contains(chd)) {
                // has encounter this child, must ensure one vertex same as partialJoinedElem[chd's index]
                val lastRstChd = partialJoinedElem(vtx2Idx(chd))
                if (matchChdUnJoined.contains(lastRstChd))
                  matchChdUnJoined -= lastRstChd // remove this matched child because is has be fixed in res
                else
                  chdFlag = false
              } else {
                chdUnJoined += chd // add child to unJoined children
                matchChdUnJoined --= partialJoinedElemSet // avoid unexpected cycle in results
              }
            }

            if (matchChdUnJoined.size < chdUnJoined.size)
              chdFlag = false
            tagFlag = chdFlag
          }

          if (tagFlag) {
            val tag2ChdFilter = tag2UnJoinedChd.filter(_._2.nonEmpty)
            if (tag2ChdFilter.isEmpty) {
              // all chd have joined, no need to modify lastRst
              val lastRst = partialJoinedElem.clone()
              lastRst(rootIdx) = rootMatch
              partitionRst += lastRst
            }
            else {
              val unJoinedTag = tag2ChdFilter.keys.toArray // unJoined swig tag
              val unJoinedChd = tag2ChdFilter.values.map(_.toArray.sorted).toArray // unJoined chd
              val unJoinedMatchChd = unJoinedTag.map(tag2UnJoinedMatchChd).map(_.toArray.sorted) //unJoined matching chd

              val lastRst = partialJoinedElem.clone()
              lastRst(rootIdx) = rootMatch
              val oneRst = joinProcess(lastRst, vtx2Idx)(unJoinedMatchChd, unJoinedChd)
              partitionRst ++= oneRst
            }
          }
        }
      }
    }
    partitionRst.toIterator
  }

  /**
    * make mixed vertex and tag use one long type, the function is same as triplet in graphx,
    * but more efficiently
    *
    * @param vertex        data graph vertex
    * @param edge          data graph edge
    * @param numPartitions partition size for parallel
    * @return new data graph edge tuple whose vertex mixed id and tag
    */
  def makeCmbEdge(vertex: RDD[(Long, Int)], edge: RDD[(Long, Long)], numPartitions: Int)
  : RDD[(Long, Long)] = {

    def zipTagEdge(vtxTagIt: Iterator[(Long, Int)], edgeIt: Iterator[(Long, Long)]): Iterator[(Long, Long)] = {
      val vtx2Tag = vtxTagIt.toMap
      val leftCmb = edgeIt.map(x => (cmbTagId(vtx2Tag(x._1), x._1), x._2))
      leftCmb
    }

    val vtx = vertex.partitionBy(new HashPartitioner(numPartitions)).cache()
    val swpEdge = edge.map(_.swap).partitionBy(new HashPartitioner(numPartitions)) //have swap
    val swpEdgeWithDstCmb = vtx.zipPartitions(swpEdge)(zipTagEdge) //have swap
    val edgeWithDstCmb = swpEdgeWithDstCmb.map(_.swap).partitionBy(new HashPartitioner(numPartitions))
    val cmbEdge = vtx.zipPartitions(edgeWithDstCmb)(zipTagEdge)
    //vtx.unpersist()

    cmbEdge
  }

  def cmbTagId(tag: Int, id: Long) = (tag.toLong << 40) + id // tag 24bit, id 40bit

  def getTag(mixId: Long) = (mixId >> 40).toInt

  def getId(mixId: Long) = 1099511627775L & mixId // 1099511627775L=2^40-1

  val cmbVtxOrdering = new Ordering[(Long, Array[Long], Double, Int)] {
    override def compare(a: (Long, Array[Long], Double, Int), b: (Long, Array[Long], Double, Int)): Int = {
      if (a._3 - b._3 < 0) -1
      else if (a._3 - b._3 == 0) 0
      else 1
    }
  }
}
