/** *****************************************************************************
  * Copyright 2013 Lars Behnke
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * *****************************************************************************/
package com.szadowsz.census.cluster

import com.szadowsz.census.cluster.cluster._

import java.util.ArrayList
import java.util.Arrays
import java.util.HashSet
import java.util.List

class DefaultClusteringAlgorithm {

  def performClustering(distances: Array[Array[Double]], clusterNames: Array[String], linkageStrategy: Nothing): Cluster = {
    checkArguments(distances, clusterNames, linkageStrategy)
    val clusters: List[Cluster] = createClusters(clusterNames)
    val linkages: DistanceMap = createLinkages(distances, clusters)
    val builder: HierarchyBuilder = new HierarchyBuilder(clusters, linkages)
    while (!builder.isTreeComplete) {
      builder.agglomerate(linkageStrategy)
    }
    return builder.getRootCluster
  }

  private def checkArguments(distances: Array[Array[Double]], clusterNames: Array[String], linkageStrategy: Nothing) {
    if (distances == null || distances.length == 0 || distances(0).length != distances.length) {
      throw new IllegalArgumentException("Invalid distance matrix")
    }
    if (distances.length != clusterNames.length) {
      throw new IllegalArgumentException("Invalid cluster name array")
    }
    if (linkageStrategy == null) {
      throw new IllegalArgumentException("Undefined linkage strategy")
    }
//    val uniqueCount: Int = new HashSet[String](Arrays.asList(clusterNames)).size
//    if (uniqueCount != clusterNames.length) {
//      throw new IllegalArgumentException("Duplicate names")
//    }
  }

  def performWeightedClustering(distances: Array[Array[Double]], clusterNames: Array[String], weights: Array[Double], linkageStrategy: Nothing): Cluster = {
    checkArguments(distances, clusterNames, linkageStrategy)
    if (weights.length != clusterNames.length) {
      throw new IllegalArgumentException("Invalid weights array")
    }
    val clusters: List[Cluster] = createClusters(clusterNames, weights)
    val linkages: DistanceMap = createLinkages(distances, clusters)
    val builder: HierarchyBuilder = new HierarchyBuilder(clusters, linkages)
    while (!builder.isTreeComplete) {
      builder.agglomerate(linkageStrategy)
    }
    return builder.getRootCluster
  }

  private def createLinkages(distances: Array[Array[Double]], clusters: List[Cluster]): DistanceMap = {
    val linkages: DistanceMap = new DistanceMap
    {
      var col: Int = 0
      while (col < clusters.size) {
        {
          {
            var row: Int = col + 1
            while (row < clusters.size) {
              {
                val link: ClusterPair = new ClusterPair
                val lCluster: Cluster = clusters.get(col)
                val rCluster: Cluster = clusters.get(row)
                link.setLinkageDistance(distances(col)(row))
                link.setlCluster(lCluster)
                link.setrCluster(rCluster)
                linkages.add(link)
              }
              ({
                row += 1; row - 1
              })
            }
          }
        }
        ({
          col += 1; col - 1
        })
      }
    }
    return linkages
  }

  private def createClusters(clusterNames: Array[String]): List[Cluster] = {
    val clusters: List[Cluster] = new ArrayList[Cluster]
    for (clusterName <- clusterNames) {
      val cluster: Cluster = new Cluster(clusterName)
      clusters.add(cluster)
    }
    return clusters
  }

  private def createClusters(clusterNames: Array[String], weights: Array[Double]): List[Cluster] = {
    val clusters: List[Cluster] = new ArrayList[Cluster]
    {
      var i: Int = 0
      while (i < weights.length) {
        {
          val cluster: Cluster = new Cluster(clusterNames(i))
          cluster.setDistance(new Distance(0.0, weights(i)))
          clusters.add(cluster)
        }
        ({
          i += 1; i - 1
        })
      }
    }
    return clusters
  }
}