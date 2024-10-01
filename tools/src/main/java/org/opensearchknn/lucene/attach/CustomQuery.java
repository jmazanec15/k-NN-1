/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearchknn.lucene.attach;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Simple script that can be used to figure out the segment id for a particular field
 */
public class CustomQuery {

    public static void main(String[] args) {
        Path dirPath = getPathToShard(args[0]);
        Path outFilePath = getPathToShard(args[1]);
        try {
            doRun(dirPath, outFilePath);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void doRun(Path path, Path outputFile) throws IOException {
        final Directory fsDirectory = FSDirectory.open(path);
        final DirectoryReader reader = DirectoryReader.open(fsDirectory);
        IndexSearcher searcher = new IndexSearcher(reader);
        Scanner sc = new Scanner(System.in);
        List<String> lines = new ArrayList<>();
        System.out.print("Enter doc ID or -1:  ");
        int docId;
        while ((docId = Integer.parseInt(sc.nextLine())) != -1) {
            Query query = IntPoint.newRangeQuery("id", docId, docId);
            Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
            for (var leafContext : searcher.getLeafContexts()) {
                int count = 0;
                DocIdSetIterator docIdSetIterator = weight.scorer(leafContext).iterator();
                while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    count++;
                }
                if (count > 0) {
                    lines.add(docId + " " + leafContext.ord);
                }
            }
        }
        Files.write(outputFile, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        fsDirectory.close();
    }

    private static Path getPathToShard(String path) {
        return Path.of(path);
    }
}
