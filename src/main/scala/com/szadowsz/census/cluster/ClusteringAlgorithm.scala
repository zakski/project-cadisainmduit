package com.szadowsz.census.cluster

import com.szadowsz.census.cluster.cluster.Cluster
import com.szadowsz.census.cluster.linkage.LinkageStrategy

trait ClusteringAlgorithm[T] {


  def performClustering(clusters: Seq[T], distances: Seq[Array[Double]], linkage: LinkageStrategy): Cluster

  def performWeightedClustering(distances: Array[Array[Double]], clusterNames: Array[String], weights: Array[Double], linkage: LinkageStrategy): Cluster
}