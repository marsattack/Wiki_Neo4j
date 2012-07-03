//
// Copyright (c) 2012 Mirko Nasato
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
package org.graphipedia.dataimport.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

public class ImportGraph {

    private final BatchInserter inserter;
    private final BatchInserterIndex index;
    private final Map<String, Long> inMemoryIndex;

    public ImportGraph(String dataDir) {
        inserter = BatchInserters.inserter(dataDir);
        final BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(inserter);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                indexProvider.shutdown();
                inserter.shutdown();
            }
        });

        index = indexProvider.nodeIndex("pages", MapUtil.stringMap("type", "exact", "to_lower_case", "false"));
        inMemoryIndex = new HashMap<String, Long>(12100000);
    }

    public void createNodes(String fileName) throws Exception {
        System.out.println("Importing pages...");
        NodeCreator nodeCreator = new NodeCreator(inserter, index, inMemoryIndex);
        long startTime = System.currentTimeMillis();
        nodeCreator.parse(fileName);
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("\n%d pages imported in %d seconds.\n", nodeCreator.getPageCount(), elapsedSeconds);
    }

    public void createRelationships(String fileName) throws Exception {
        System.out.println("Importing links...");
        RelationshipCreator relationshipCreator = new RelationshipCreator(inserter, inMemoryIndex);
        long startTime = System.currentTimeMillis();
        relationshipCreator.parse(fileName);
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("\n%d links imported in %d seconds; %d broken links ignored\n",
                relationshipCreator.getLinkCount(), elapsedSeconds, relationshipCreator.getBadLinkCount());
    }

}
