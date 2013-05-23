package org.elasticsearch.thrift.test;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.BlockGroupingCollector;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.Version;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Dong ai hua
 * Date: 13-4-19
 * Time: 下午5:16
 * To change this template use File | Settings | File Templates.
 */
public class LuceneTests {
    private Directory dir, taxoDir;
    private IndexWriter writer;

    //private DirectoryReader reader;
    //private IndexSearcher searcher;
    private SearcherFactory searcherFactory = new SearcherFactory();
    private SearcherManager searcherManager;

    private StandardAnalyzer analyzer;


    private IndexWriterConfig config;

    private int hitsPerPage = 10;

    private TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);

    private void index() throws Exception {
        addDoc(writer, "Lucene in Action", "193398817");
        addDoc(writer, "Lucene in Action", "193398818");
        addDoc(writer, "Lucene in Action", "193398819");

        addDoc(writer, "Lucene for Dummies", "55320055Z");
        addDoc(writer, "Managing Gigabytes", "55063554A");
        addDoc(writer, "The Art of Computer Science", "9900333X");
    }

    private ScoreDoc[] search( String string) throws Exception {
        //IndexReader reader = DirectoryReader.open(dir);
        //IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new QueryParser(Version.LUCENE_42, "title", analyzer).parse(string);
        searcherManager.maybeRefresh();
        IndexSearcher searcher = searcherManager.acquire();
        searcher.search(query, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;

        System.out.println("Found " + hits.length + " hits.");
        for(int i=0;i<hits.length;++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("isbn") + "\t" + d.get("title"));
            System.out.flush();
        }

        return hits;
    }

    private void printHits(IndexSearcher searcher, ScoreDoc[] hits) throws Exception{
        System.out.println("Found " + hits.length + " hits.");
        for(int i=0;i<hits.length;++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("isbn") + "\t" + d.get("title"));
            System.out.flush();
        }
    }

    @BeforeMethod
    public void setupIndexWriterSearcher() throws Exception, TTransportException {
        dir = new RAMDirectory();
        taxoDir = new RAMDirectory();
        analyzer = new StandardAnalyzer(Version.LUCENE_42);
        config = new IndexWriterConfig(Version.LUCENE_42, analyzer);
        writer = new IndexWriter(dir, config);
        writer.commit();

        //reader = DirectoryReader.open(dir);
        //searcher = new IndexSearcher(reader);
        searcherManager = new SearcherManager(writer, true, searcherFactory);

        index();
    }

    @AfterMethod
    public void close() throws IOException {
       // reader.close();
        writer.close();
    }

    @Test
    public void testIndex() throws  Exception {
        index();
        search( "art");
    }

    @Test
    public void testHighlight() throws Exception {
        index();
        //IndexReader reader = DirectoryReader.open(dir);
        //IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new QueryParser(Version.LUCENE_42, "title", analyzer).parse("art");

        searcherManager.maybeRefresh();
        IndexSearcher searcher = searcherManager.acquire();
        searcher.search(query, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
        Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));
        for (int i = 0; i < hits.length; i++) {
            int id = hits[i].doc;
            Document doc = searcher.doc(id);

            String text = doc.get("title");
            TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), id, "title", analyzer);
            TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, false, 10);//highlighter.getBestFragments(tokenStream, text, 3, "...");
            for (int j = 0; j < frag.length; j++) {
                if ((frag[j] != null) && (frag[j].getScore() > 0)) {
                    System.out.println((frag[j].toString()));
                }
            }
            //Term vector
            text = doc.get("title");
            tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), hits[i].doc, "title", analyzer);
            frag = highlighter.getBestTextFragments(tokenStream, text, false, 10);
            for (int j = 0; j < frag.length; j++) {
                if ((frag[j] != null) && (frag[j].getScore() > 0)) {
                    System.out.println((frag[j].toString()));
                }
            }
            System.out.println("-------------");
        }
    }

    @Test
    public void testFacet() throws Exception {
    /*
        TaxonomyWriter taxo = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);
        Document doc = new Document();
        doc.add(new TextField("title", "jackie", Field.Store.YES));
        List<CategoryPath> categories = new ArrayList<CategoryPath>();
        categories.add(new CategoryPath("author", "Mark Twain"));
        categories.add(new CategoryPath("year", "2010"));
        DocumentBuilder categoryDocBuilder = new CategoryDocumentBuilder(taxo);
        categoryDocBuilder.setCategoryPaths(categories);
        categoryDocBuilder.build(doc);
        writer.addDocument(doc);
        */
    }

    @Test
    public void testTwoPassGrouping() throws Exception {
        Sort groupSort = new Sort();
        Boolean fillFields = true;

        Boolean useCache = true;
        double cacheSize = 4.0;
        Boolean cacheScores = true;

        Boolean requiredTotalGroupCount = true;
        int groupOffset = 0;
        int groupLimit = 10;
        String groupField = "title";
        String field = "title";
        String qString = "Lucene";

        GroupingSearch groupingSearch = new GroupingSearch(groupField);
        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setFillSortFields(fillFields);

        if (useCache) {
            // Sets cache in MB
            groupingSearch.setCachingInMB(cacheSize, cacheScores);
        }

        if (requiredTotalGroupCount) {
            groupingSearch.setAllGroups(true);
        }

        Query query = new QueryParser(Version.LUCENE_42, field, analyzer).parse(qString);
        //TermQuery query = new TermQuery(new Term("content", searchTerm));

        searcherManager.maybeRefresh();
        IndexSearcher searcher = searcherManager.acquire();
        searcher.search(query, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        printHits(searcher, hits);

        TopGroups<BytesRef> result = groupingSearch.search(searcher, query, groupOffset, groupLimit);

        // Render groupsResult...
        if (requiredTotalGroupCount) {
            int totalGroupCount = result.totalGroupCount;
            System.out.println("Total groups:" + totalGroupCount);
            for(int i=0; i<result.groups.length; i++){
                printHits(searcher, result.groups[i].scoreDocs);
            }
        }
    }

    @Test
    public void testOnePassGrouping() throws Exception {
        int topNGroups = 10;
        Boolean needsScores = true;
        String field = "groupEnd";
        String value = "x";
        Sort groupSort = new Sort();
        Sort withinGroupSort = new Sort();
        int groupOffset = 0;

        int docOffset = 0;
        int docsPerGroup = 100;
        Boolean fillFields = true;

        int groupLimit = 100;

        // Create Documents from your source:
        List<Document> oneGroup = new ArrayList<Document>();
        Document doc = new Document();
        doc.add(new TextField("title", "Lucene in action", Field.Store.YES));
        // use a string field for isbn because we don't want it tokenized
        doc.add(new StringField("isbn", "12345", Field.Store.YES));

        oneGroup.add(doc);

        Field groupEndField = new Field(field, value, Field.Store.NO, Field.Index.NOT_ANALYZED);
        oneGroup.get(oneGroup.size()-1).add(groupEndField);

        // You can also use writer.updateDocuments(); just be sure you
        // replace an entire previous doc block with this new one.  For
        // example, each group could have a "groupID" field, with the same
        // value for all docs in this group:
        writer.addDocuments(oneGroup);

        // Set this once in your app & save away for reusing across all queries:
        Filter groupEndDocs = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term(field, value))));

        searcherManager.maybeRefresh();
        IndexSearcher searcher = searcherManager.acquire();

        // Per search:
        BlockGroupingCollector collector1;
        collector1 = new BlockGroupingCollector(groupSort,  groupOffset+topNGroups, needsScores,  groupEndDocs);
        searcher.search(new TermQuery(new Term("title", "Lucene")), collector1);

        TopGroups groupsResult = collector1.getTopGroups(withinGroupSort, groupOffset, docOffset, docOffset+docsPerGroup, fillFields);
        printHits(searcher, groupsResult.groups[0].scoreDocs);

        // Render groupsResult... Or alternatively use the GroupingSearch convenience utility:
        // Per search:
        GroupingSearch groupingSearch = new GroupingSearch(groupEndDocs);
        groupingSearch.setGroupSort(groupSort);
        groupingSearch.setIncludeScores(needsScores);
        TermQuery query = new TermQuery(new Term("title", "Lucene"));
        TopGroups groupsResult2 = groupingSearch.search(searcher, query, groupOffset, groupLimit);

        printHits(searcher, groupsResult2.groups[0].scoreDocs);


    }

    @Test
    public void testTermAllGroupHeadsCollector() throws Exception {
        String groupField = "title";
        Sort sortWithinGroup = new Sort();

        searcherManager.maybeRefresh();
        IndexSearcher searcher = searcherManager.acquire();

        // Render groupsResult... Note that the groupValue of each GroupDocs will be null, so if you need to present this value you'll have to separately retrieve it (for example using stored fields, FieldCache, etc.).
        //Another collector is the TermAllGroupHeadsCollector that can be used to retrieve all most relevant documents per group. Also known as group heads. This can be useful in situations when one wants to compute group based facets / statistics on the complete query result. The collector can be executed during the first or second phase. This collector can also be used with the GroupingSearch convenience utility, but when if one only wants to compute the most relevant documents per group it is better to just use the collector as done here below.
        AbstractAllGroupHeadsCollector collector = TermAllGroupHeadsCollector.create(groupField, sortWithinGroup);
        searcher.search(new TermQuery(new Term("title", "Lucene")), collector);

        // Return all group heads as int array
        int[] groupHeadsArray = collector.retrieveGroupHeads();
        // Return all group heads as FixedBitSet.
        int maxDoc = searcher.getIndexReader().maxDoc();
        FixedBitSet groupHeadsBitSet = collector.retrieveGroupHeads(maxDoc);

    }

    @Test
    public void testFiedCache() throws Exception {
    }

    @Test
    public void testFieldSelector() throws Exception {
    }

    private static void addDoc(IndexWriter w, String title, String isbn) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("title", title, Field.Store.YES));
        // use a string field for isbn because we don't want it tokenized
        doc.add(new StringField("isbn", isbn, Field.Store.YES));
        w.addDocument(doc);
    }
}
