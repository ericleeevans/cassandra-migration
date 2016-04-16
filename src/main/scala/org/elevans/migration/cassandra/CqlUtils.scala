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

import scala.annotation.tailrec

/**
 * Utilities for working with CQL files.
 *
 * @author Eric Evans
 */
object CqlUtils {

  /**
   * Remove all CQL comments from the given input (such as a CQL script). This includes:
   *  - single-line comments beginning with "--" or "//" and ending with the end-of-line.
   *  - multi-line or 'block' comments beginning with "/*" and ending with "*/"
   *
   * Comment-like text that is inside CQL string literals (i.e., within single-quotes) is NOT removed.
   * Single-quoted text with a comment is part of the comment and is removed with it.
   *
   * @param input String containing CQL text, possibly including comments
   *
   * @return the input String, stripped of CQL comments
   */
  def stripComments(input: String): String = {
    @tailrec
    def stripper(input: List[Char], output: List[Char], inLiteral: Boolean, inLineComment: Boolean, inBlockComment: Boolean): List[Char] = input match {
      case Nil => output
      case c :: remainder =>
        val lineCommentOpened  = c == '-' && remainder.headOption == Some('-') || c == '/' && remainder.headOption == Some('/')
        val lineCommentClosed  = c == '\n'
        val blockCommentOpened = c == '/' && remainder.headOption == Some('*')
        val blockCommentClosed = c == '*' && remainder.headOption == Some('/')

        val nowInLineComment  = ( !inLiteral && !inBlockComment && lineCommentOpened ) || ( inLineComment && !lineCommentClosed )
        val nowInBlockComment = ( !inLiteral && !inLineComment && blockCommentOpened ) || ( inBlockComment && !blockCommentClosed )
        val nowInLiteral = if (!inLineComment && !inBlockComment && c == '\'') !inLiteral else inLiteral

        val (in, out) = if (nowInLineComment || nowInBlockComment)
          (remainder, output)  // Discard c
        else if (inBlockComment && !nowInBlockComment)
          (remainder.drop(1), output)  // Discard c ('*') and the next Char '/')
        else
          (remainder, output :+ c)

        stripper(in, out, nowInLiteral, nowInLineComment, nowInBlockComment)
    }

    stripper(input.toList, Nil, false, false, false).mkString
  }
}


