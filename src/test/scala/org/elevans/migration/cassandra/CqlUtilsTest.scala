// Copyright 2015-2016 Eric Evans : Scala migration
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.elevans.migration.cassandra

import org.scalatest.{Matchers, FunSpec}

class CqlUtilsTest extends FunSpec with Matchers {

  describe("stripComments") {

    it("removes CQL single-line comments") {
      val input = """
hickory--xxx
  dickory-- xxx
    dock-- xxx-- stuff
-- xxx
// xxx
the mouse//xxx
  ran up// 'xxx
    the clock// 'xxx'// stuff"""

      val output = """
hickory
  dickory
    dock


the mouse
  ran up
    the clock"""

      CqlUtils.stripComments(input) shouldBe output
    }


    it("removes CQL block comments") {
      val input = """
hickory/* xxx */
/* xxx */  dickory
    dock/*
xxx
*/
/* xxx */
/*
 xxx
*/
the /* xxx */mouse/* -- 'xxx */
  ran/* 'xxx */ up/* xxx' /* yyy */
    the clock-- /* xxx """

      val output = """
hickory
  dickory
    dock


the mouse
  ran up
    the clock"""

      CqlUtils.stripComments(input) shouldBe output
    }


    it("does not remove CQL comment-style text within string literals") {
      val input = """
'hickory--'
'  --dickory --
    //dock //'
'the mouse /* !!! */'
'  /* !!! */ran ''up''
    the /* !!! */ clock'"""

      CqlUtils.stripComments(input) shouldBe input
    }
  }
}
