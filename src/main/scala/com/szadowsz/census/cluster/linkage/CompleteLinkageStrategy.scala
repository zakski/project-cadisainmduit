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
package com.szadowsz.census.cluster.linkage

import com.szadowsz.census.cluster.cluster.Distance

import scala.collection.Seq

/**
 *  In complete-linkage clustering, the link between two clusters contains all element pairs, and the distance between
 *  clusters equals the distance between those two elements (one in each cluster) that are farthest away from each
 *  other. The shortest of these links that remains at any step causes the fusion of the two clusters whose elements
 *  are involved. The method is also known as farthest neighbour clustering.
 */
class CompleteLinkageStrategy extends LinkageStrategy {
  def calculateDistance(distances: Seq[Distance]): Distance = {
    new Distance(distances.foldLeft(Double.MinValue) { (m, dist) => if (dist.getDistance > m) dist.getDistance else m })
  }
}