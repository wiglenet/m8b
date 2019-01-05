package net.wigle.m8b;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import net.wigle.m8b.siphash.SipHash;
import net.wigle.m8b.siphash.SipKey;
import net.wigle.m8b.geodesy.utm;
import net.wigle.m8b.geodesy.mgrs;

/* 
 * Copyright (c) 2018, Hugh Kennedy, Robert Hagemann, Andrew Carra
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the WiGLE.net nor Mimezine nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
public final class m8b {

    /**
     * command line magic8ball shaker.
     *
     * generate - read in mac|lat|lon text file, produce m8b
     * stage    - read in mac|lat|lon text file, produce intermediate output files for further reduction. 
     * restage  - read in mac|lat|lon text file, produce reduced intermediate output files for further reduction. 
     * reduce   - sort and reduce stage files
     * compact  - sort and reduce stage files without keying
     * combine  - assemble reduced files, produce m8b
     * dumpi    - dump intermediate file to stdout
     * query    - read entire m8b query resulting datastructure for macs
     * scan     - read data from m8b based on macs, return query results
     * score    - read stage files, establish stats
     * score2   - read stage files, establish other stats
     */
    public static void main(String[] argv) throws Exception {

	String cmd = "";
	if (argv.length > 0){
	    cmd = argv[0];
	}
	
	switch(cmd) {
	case "generate": {
	    System.out.println("do generate read "+argv[1]+" write "+argv[2]+" slice "+argv[3]);
	    boolean tabs = (argv.length > 4 && "-t".equals(argv[4]));
	    generate(argv[1],argv[2],Integer.parseInt(argv[3]),tabs);
	    break;
	}
	    
	case "stage":{
	    System.out.println("do stage read "+argv[1]+" write "+argv[2]);
	    boolean tabs = (argv.length > 3 && "-t".equals(argv[3]));
	    stage(argv[1],argv[2],tabs);
	    break;
	}

	case "restage":{
	    System.out.println("do restage read "+argv[1]+" write "+argv[2]);
	    boolean tabs = (argv.length > 3 && "-t".equals(argv[3]));
	    restage(argv[1],argv[2],tabs);
	    break;
	}
	    
	case "score":{
	    System.out.println("do score read "+argv[1]);
	    score(argv[1]);
	    break;
	}

	case "score2":{
	    System.out.println("do score2 read "+argv[1]);
	    score2(argv[1]);
	    break;
	}

	    
	case "reduce": {
	    System.out.println("do reduce read "+argv[1]+" write "+argv[2]+" slice "+argv[3]);
	    reduce(argv[1],argv[2],Integer.parseInt(argv[3]));
	    break;
	}

	case "compact": {
	    System.out.println("do compact read "+argv[1]);
	    compact(argv[1]);
	    break;
	}

	    
	case "combine": {
	    System.out.println("do combine read "+argv[1]+" write "+argv[2]+" slice "+argv[3]);
	    combine(argv[1],argv[2],Integer.parseInt(argv[3]));
	    break;
	}

	case "query":{
	    System.out.println("do query read "+argv[1]+" check "+Arrays.stream(argv).skip(2).collect(Collectors.joining(", ")));
	    query(argv[1],Arrays.copyOfRange(argv,2,argv.length));
	    break;
	}
	case "scan":{
	    System.out.println("do scan read "+argv[1]+" check "+Arrays.stream(argv).skip(2).collect(Collectors.joining(", ")));
	    scan(argv[1],Arrays.copyOfRange(argv,2,argv.length));
	    break;
	}

	case "dumpi":{
	    System.out.println("do dump intermediate read "+argv[1]);
	    dumpi(argv[1]);
	    break;
	}

	default:{
	    System.err.println("m8b generate observation-filename m8b-filename slice-bits [-t]");
	    System.err.println("m8b stage observation-filename stage-location/ [-t]");
	    System.err.println("m8b restage observation-filename stage-location/ [-t]");
   	    System.err.println("m8b score stage-location/");
	    System.err.println("m8b score2 stage-location/");
	    System.err.println("m8b reduce stage-location/ reduce-location/ slice-bits");
	    System.err.println("m8b compact stage-location/");
	    System.err.println("m8b combine reduce-location/ m8b-filename slice-bits");
	    System.err.println("m8b dumpi intermediate-filename");
	    System.err.println("m8b query m8b-filename mac1 [... macN]");
	    System.err.println("m8b scan m8b-filename mac1 [... macN]");

	    break;
	}
	}
    }

    /**
     * read mac|lat|lon per line text file (skipping first header line)
     * convert into m8b data structure, write out to file
     */
    private static void generate(String fromFile, String toFile, int slicebits,boolean tabs) throws Exception {
	Stream<String> lines = Files.lines(new File(fromFile).toPath()).skip(1);

	// just zerokey it. we're not trying to avoid collisions.
	SipKey sipkey = new SipKey(new byte[16]);
	byte[] macbytes = new byte[6];
	Map<Integer,Set<mgrs>> mjg = new TreeMap<Integer,Set<mgrs>>();
	
	int non_utm=0;

	int records = 0;

	char sep = tabs ? '\t' : '|';
	
        for (Iterator<String> it = lines.iterator(); it.hasNext(); )
        {

	    //bssid|bestlat|bestlon
	    //8e:15:44:60:50:ac|40.00900289|-75.21358834
	    
	    String line = it.next();

	    int b1 = line.indexOf(sep);
	    int b2 = line.indexOf(sep,b1+1);

	    String latstr = line.substring(b1+1,b2);
	    String lonstr = line.substring(b2+1);

	    double lat = Double.parseDouble(latstr);
	    double lon = Double.parseDouble(lonstr);

	    if (!(-80<=lat && lat<=84)) {
		non_utm++;
		continue;
	    }

	    mgrs m = mgrs.fromUtm(utm.fromLatLon(lat,lon));

            String slice2 = line.substring(0,17);
	    Integer kslice2 = extractKeyFrom(slice2,macbytes,sipkey,slicebits);

	    Set<mgrs> locs = mjg.get(kslice2);
	    if (locs==null){
		locs = new HashSet<mgrs>();
		mjg.put(kslice2,locs);
	    }
	    if(locs.add(m)){
		records++;
	    }
	}
	// mjg is complete, write out to pairfile

	Charset utf8  = Charset.forName("UTF-8");
	SeekableByteChannel out = Files.newByteChannel(new File(toFile).toPath(), EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));

        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
    
	// write header
	bb.put("MJG\n".getBytes(utf8)); // magic number
	bb.put("2\n".getBytes(utf8)); // version
	bb.put("SIP-2-4\n".getBytes(utf8)); // hash
	bb.put(String.format("%x\n",slicebits).getBytes(utf8)); // slice bits (hex)
	bb.put("MGRS-1000\n".getBytes(utf8)); // coords
	bb.put("4\n".getBytes(utf8)); // id size in bytes (hex)
	bb.put("9\n".getBytes(utf8)); // coords size in bytes (hex)
	bb.put(String.format("%x\n",records).getBytes(utf8)); // record count (hex)

	int recordsize = 4+9;
	
	bb.flip();
	while (bb.hasRemaining()){
	    out.write(bb);
	}

	// back to fill mode
	bb.clear();
	byte[] mstr = new byte[9];
	for ( Map.Entry<Integer,Set<mgrs>> me : mjg.entrySet()) {
	    int key = me.getKey().intValue();
	    for ( mgrs m : me.getValue() ) {

		if (bb.remaining() < recordsize ) {
		    bb.flip();
		    while (bb.hasRemaining()){
			out.write(bb);
		    }
		    bb.clear();
		}
		m.populateBytes(mstr);
		bb.putInt(key).put(mstr);
	    }
	}
	
	bb.flip();
	while (bb.hasRemaining()) {
	    out.write(bb);
	}
	bb.clear();
	out.close();
    }


    /**
     * read mac|lat|lon per line text file (skipping first header line)
     * convert into multiple file intermediate m8b data at stageLoc
     * no slicing of ids, reduces to ~30% of source data size.
     * 
     * stage splits fromFile into 16 files 
     */
    private static void stage(String fromFile, String stageLoc, boolean tabs) throws Exception {

	Stream<String> lines = Files.lines(new File(fromFile).toPath()).skip(1);

	// just zerokey it. we're not trying to avoid collisions.
	SipKey sipkey = new SipKey(new byte[16]);
	byte[] macbytes = new byte[6];


	ByteBuffer[] bb = new ByteBuffer[16];
	SeekableByteChannel[] out = new SeekableByteChannel[bb.length];


	for (int i=0;i<bb.length;i++) {
	    bb[i] = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
	    out[i] = Files.newByteChannel(new File(stageLoc,"stage_"+i).toPath(), EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
	}
	
	int non_utm=0;

	int records = 0;
	int[] rslice = new int[bb.length];

	char sep = tabs ? '\t' : '|';
	
	int recordsize = 4+9;
	byte[] mstr = new byte[9];
        for (Iterator<String> it = lines.iterator(); it.hasNext(); )
        {

	    //bssid|bestlat|bestlon
	    //8e:15:44:60:50:ac|40.00900289|-75.21358834
	    
	    String line = it.next();

	    int b1 = line.indexOf(sep);
	    int b2 = line.indexOf(sep,b1+1);

	    String latstr = line.substring(b1+1,b2);
	    String lonstr = line.substring(b2+1);

	    double lat = Double.parseDouble(latstr);
	    double lon = Double.parseDouble(lonstr);

	    if (!(-80<=lat && lat<=84)) {
		non_utm++;
		continue;
	    }

	    mgrs m = mgrs.fromUtm(utm.fromLatLon(lat,lon));

            String slice2 = line.substring(0,17);
	    int key = extractIntKeyFrom(slice2,macbytes,sipkey,32);
	    int idx = (records & 0x0f); // straight round robin

	    if (bb[idx].remaining() < recordsize ) {
		bb[idx].flip();
		while (bb[idx].hasRemaining()){
		    out[idx].write(bb[idx]);
		}
		bb[idx].clear();
	    }
	    m.populateBytes(mstr);
	    bb[idx].putInt(key).put(mstr);
	    records++;
	    rslice[idx]++;
	}

	// done. do last write/flush
	for (int i=0;i<bb.length;i++) {
	    bb[i].flip();
	    while (bb[i].hasRemaining()){
		out[i].write(bb[i]);
	    }
	    bb[i].clear();
	    
	    out[i].close();
	}
	
	System.out.println("there were "+non_utm+" out of bounds records, and a total of "+records+" written "+(records*recordsize) +" bytes");
	for(int i = 0; i< rslice.length; i++){
	    System.out.println(i+" => "+rslice[i]);
	}
    }

    /**
     * combine stage()/compact() into one action, avoiding one intermediate set of read/writes.
     *
     * read mac|lat|lon per line text file (skipping first header line)
     * convert into multiple file intermediate m8b data at stageLoc
     * no slicing of ids, reduces to ~30% of source data size.
     * 
     * stage splits fromFile into 16 files, by unsliced hash, and sort/reduces them.
     */
    private static void restage(String fromFile, String stageLoc, boolean tabs) throws Exception {

	Stream<String> lines = Files.lines(new File(fromFile).toPath()).skip(1);

	// just zerokey it. we're not trying to avoid collisions.
	SipKey sipkey = new SipKey(new byte[16]);
	byte[] macbytes = new byte[6];


	ByteBuffer[] bb = new ByteBuffer[16];
	SeekableByteChannel[] out = new SeekableByteChannel[bb.length];

	Path[] stage = new Path[bb.length];
	Path[] reduce2 = new Path[bb.length];

	for (int i=0;i<bb.length;i++) {
	    bb[i] = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
	    stage[i] = new File(stageLoc,"stage_"+i).toPath();
	    out[i] = Files.newByteChannel(stage[i], EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
	    reduce2[i] = new File(stageLoc,"reduce2_"+i).toPath();
	}
	
	int non_utm=0;

	int records = 0;
	int[] rslice = new int[bb.length];

	char sep = tabs ? '\t' : '|';
	
	int recordsize = 4+9;
	byte[] mstr = new byte[9];
        for (Iterator<String> it = lines.iterator(); it.hasNext(); )
        {

	    //bssid|bestlat|bestlon
	    //8e:15:44:60:50:ac|40.00900289|-75.21358834
	    
	    String line = it.next();

	    int b1 = line.indexOf(sep);
	    int b2 = line.indexOf(sep,b1+1);

	    String latstr = line.substring(b1+1,b2);
	    String lonstr = line.substring(b2+1);

	    double lat = Double.parseDouble(latstr);
	    double lon = Double.parseDouble(lonstr);

	    if (!(-80<=lat && lat<=84)) {
		non_utm++;
		continue;
	    }

	    mgrs m = mgrs.fromUtm(utm.fromLatLon(lat,lon));

            String slice2 = line.substring(0,17);
	    int key = extractIntKeyFrom(slice2,macbytes,sipkey,32);

	    int idx = (int)((key >> 28) & 0x0f);

	    if ( bb[idx].remaining() < recordsize ) {
		bb[idx].flip();
		while (bb[idx].hasRemaining()){
		    out[idx].write(bb[idx]);
		}
		bb[idx].clear();
	    }
	    m.populateBytes(mstr);
	    bb[idx].putInt(key).put(mstr);
	    records++;
	    rslice[idx]++;
	}

	int max = -1;
	// done. do last write/flush
	for (int i=0;i<bb.length;i++) {
	    bb[i].flip();
	    while (bb[i].hasRemaining()){
		out[i].write(bb[i]);
	    }
	    bb[i].clear();
	    
	    out[i].close();

	    if (rslice[i] > max){
		max = rslice[i];
	    }
	}
	
	System.out.println("there were "+non_utm+" out of bounds records, and a total of "+records+" written "+(records*recordsize) +" bytes");
	for(int i = 0; i< rslice.length; i++){
	    System.out.println(i+" => "+rslice[i]);
	}
	
	ByteBuffer ib = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java

	// should we bound this max? secondary option is multifile sortmerge.
	System.out.println("max: "+max+", allocating ~"+(max*(recordsize+8))+"b");
	byte[][] entries = new byte[max][];
	for ( int i = 0;i<entries.length;i++){
	    entries[i] = new byte[recordsize];
	}
	int dups[] = new int[stage.length];


	for (int i =0;i<stage.length;i++ ) {
	    Path entry = stage[i];
	    SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));//,);
	    // 
	    int read = in.read(ib);
	    int idx=0;
	    while (read > 0) {
		ib.flip();
		
		while ( ib.remaining() > recordsize ) {
		    ib.get(entries[idx],0,recordsize); // guaranteed to have recordsize from the conditional and bytebuffer.get will fill what is available.
		    idx++;
		}
		ib.compact(); // partial reads.
		read = in.read(ib);
	    }
	    // sort entries by keybits from 0,idx
	    Arrays.sort(entries,0,idx,CMP);

	    SeekableByteChannel outc = Files.newByteChannel(reduce2[i], EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
	    ib.clear();
	    int lastwrite=-1;
	    for ( int j = 0; j < idx; j++ ) {
		if (ib.remaining() < recordsize ) {
		    ib.flip();
		    while (ib.hasRemaining()){
			outc.write(ib);
		    }
		    ib.clear();
		}
		// suppress dups
		if ( lastwrite >= 0 ) {
		    if (CMP.compare(entries[lastwrite],entries[j]) == 0) {
			dups[i]++;
			continue;
		    }
		}
		
		ib.put(entries[j],0,recordsize);
		lastwrite = j;
	    }
	    // done. do last write and flush
	    ib.flip();
	    while (ib.hasRemaining()){
		outc.write(ib);
	    }
	    ib.clear();
	    outc.close();
	}
	System.out.println("dups suppressed: "+Arrays.stream(dups).mapToObj(x->Integer.toString(x)).collect(Collectors.joining(", ")));

	// shuffle stage->oldstage, reduce2->stage, remove oldstage
	for (int i = 0; i < reduce2.length; i++) {
	    Path oldstage = new File(stageLoc,"oldstage_"+i).toPath();
	    Files.move(stage[i],oldstage);
	    Files.move(reduce2[i],stage[i]);
	    Files.delete(oldstage);
	}


    }

    
    /**
     * read multiple file intermediate m8b data at stageLoc
     * establish and report stats on the dataset
     *
     */
    private static void score(String stageLoc) throws Exception {
	// first cut:
	//   read each input file, populate
	//     coord->index
	//     coord->list of hashes
	//     hash->bitset of coord indexes
	//   for each coord
	//     tscore = cscore = 0
	//     for (i=0;i<hash.count;i++) // PIHD
	//        bi = hash[i].bitset;
	//        for (j=i+1;j<hash.count;j++)
	//          bj = hash[j].bitset
	//          c = bi.and(bj).cardinality() // hamming distance
	//          if (c>0) // 0 = nothing in common
	//             tscore += c
	//             cscore++
	//     coord -> mscore = tscore/cscore; // median score, lower is better

	Charset utf8  = Charset.forName("UTF-8");
	int recordsize = 4+9;
	int mgrsize = 9;
	byte[] mgrs = new byte[mgrsize];

	// first entry in the list is the index
	Map<String,List<Integer>> coordh = new HashMap<String,List<Integer>>();
	Map<Integer,int[]> hashb = new HashMap<Integer,int[]>();

	int keycollide = 0;
	int records= 0;
	int max=0;
        int cmax =0;
	
	ByteBuffer ib = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(new File(stageLoc).toPath(), "stage_*")) {
           for ( Path entry : stream ) {
	       System.out.print('.');
	       SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));//,);
	       // 
	       int read = in.read(ib);
	       while (read > 0) {
		   ib.flip();

		   while ( ib.remaining() > recordsize ) {
		       records++;
		       Integer fullk = ib.getInt();
		       ib.get(mgrs,0,mgrs.length);
		       String mgrst = new String(mgrs,0,mgrsize,utf8);
		       List<Integer> idx_h = coordh.get(mgrst);
		       Integer idx;
		       if ( null == idx_h ) {
			   idx = coordh.size();
			   idx_h = new ArrayList<>();
			   idx_h.add(idx);
			   coordh.put( mgrst, idx_h );
		       } else {
			   idx = idx_h.get(0);
		       }
		       idx_h.add(fullk);
                       if(idx_h.size() > cmax){
                          cmax = idx_h.size();
                       }

		       int[] bs = hashb.get(fullk);
		       if ( bs == null ) {
			   bs = new int[1];
			   hashb.put(fullk,bs);
		       } else {
			   keycollide++;
		       }
		       bs[0]++;
		       if ( bs[0] > max ) {
			   max = bs[0];
		       }
		   }
		   ib.compact(); // partial reads.
		   read = in.read(ib);
	       }
	   }
	}
	System.out.println("\nread:"+records+" hashes:"+hashb.size()+" cmax:"+cmax+" collisions:"+keycollide+" coords:"+coordh.size());

	List<Map.Entry<Integer,int[]>> es = new ArrayList<>(hashb.size()); // damn you java
	es.addAll(hashb.entrySet());
        Comparator<Map.Entry<Integer,int[]>> cmp = Comparator.comparingInt((Map.Entry<Integer,int[]> me)->me.getValue()[0]).reversed();
	es.sort(cmp);

	for(int i = 0;i < 10; i++){
	    System.out.println(es.get(i).getKey()+" = "+es.get(i).getValue()[0]);
	}

	// mgrs 100k density map for coords
	Map<String,int[]> dense = new HashMap<>(60*20);
	int dmax = 0;
	for ( Map.Entry<String,List<Integer>> kvp : coordh.entrySet() ) {
	    String mgrs100k = kvp.getKey().substring(0,3);
	    int[] d = dense.get(mgrs100k);
	    if (d == null) {
		d = new int[1];
		dense.put(mgrs100k,d);
	    }
	    d[0] += kvp.getValue().size() - 1; // hashes at this coord
	    if (d[0] > dmax){
		dmax = d[0];
	    }
	}
	double dec = ((dmax+1)/10.0);
	String[] zones = new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60" };
	String[] bands = new String[]{ "W","V","U","T","S","R","Q","P","O","N","M","L","K","J","I","H","G","F","E","D","C"};
	char[] fill = {' ','\u25AB','\u25AA','\u25A1','\u25F0','\u25F3','\u25F2','\u25F1','\u25EB','\u25A4','\u25EA'};
	// walk the dense map 01W -> 60W \n 01V -> 60V
        System.out.print("world 100k density scale: 0[");
	for ( char c : fill ) {
	    System.out.print(c);
	    System.out.print(' ');
	}
	System.out.println("]10 ("+dmax+")");
	System.out.println(" 01                                                          60");
	System.out.println(" +------------------------------------------------------------+");
	for (int b = 0; b < bands.length; b++) {
	    System.out.print((b==0||b==bands.length-1)?bands[b] : ' ');
	    System.out.print('|');
	    for (int z = 0;z<zones.length;z++) {
		// test
		String key = zones[z]+bands[b];
		int[] d = dense.get(key);
		if (d==null){
		    System.out.print(fill[0]); // nothing!
		} else {
		    int v = (int)(d[0]/dec)+1;
		    System.out.print(fill[v]);
		}
	    }
	    System.out.println("|");
	}
	System.out.println(" +------------------------------------------------------------+");
	
    }

    /**
     * read multiple file intermediate m8b data at stageLoc.
     * this needs to be run on the already deduped stage data.
     * establish and report stats on the dataset
     *
     */
    private static void score2(String stageLoc) throws Exception {
	// second cut:
	// look at overlapping density as histogram
	//   read each input file, populate
	//     coord->list of hashes
	//     hash->list of coords
	//   for each coord c1
	//     n = count of hash per c1
	//     for each hash h1 per c1
	//       for each coord c2 per h1
	//         histogram[c2]++
	//     score(c1) = f(histogram)
	//     -- some factor of shape of histogram
	//     -- histogram[c1] = n by definition
	//     -- if histogram[x] == n, then x dominates c1. this relation is a directed graph. ideal is minimal dominated set.
	  
	Charset utf8  = Charset.forName("UTF-8");
	int recordsize = 4+9;
	int mgrsize = 9;
	byte[] mgrs = new byte[mgrsize];

	Map<String,List<Integer>> coordh = new HashMap<String,List<Integer>>();
	Map<Integer,List<Integer>> hashc = new HashMap<Integer,List<Integer>>();

	int records= 0;
	int max=0;
        int cmax =0;
	
	ByteBuffer ib = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(new File(stageLoc).toPath(), "stage_*")) {
           for ( Path entry : stream ) {
	       System.out.print('.');
	       SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));

	       int read = in.read(ib);
	       while (read > 0) {
		   ib.flip();

		   while ( ib.remaining() > recordsize ) {
		       records++;
		       Integer fullk = ib.getInt();
		       ib.get(mgrs,0,mgrs.length);
		       String mgrst = new String(mgrs,0,mgrsize,utf8);
		       List<Integer> idx_h = coordh.get(mgrst);
		       Integer idx;
		       if ( null == idx_h ) {
			   idx_h = new ArrayList<>();
			   idx = coordh.size();
			   coordh.put( mgrst, idx_h );
			   idx_h.add(idx);
		       } else {
			   idx = idx_h.get(0);
		       }
		       idx_h.add(fullk);
                       if(idx_h.size() > cmax){
                          cmax = idx_h.size();
                       }

		       List<Integer> cs = hashc.get(fullk);
		       if ( cs == null ) {
			   cs = new ArrayList<>();
			   hashc.put(fullk,cs);
		       }
		       cs.add(idx);
		       if (cs.size() > max){
			   max = cs.size();
		       }
		   }
		   ib.compact(); // partial reads.
		   read = in.read(ib);
	       }
	   }
	}
	System.out.println("\nread:"+records+" hashes:"+hashc.size()+" max:"+max+" cmax:"+cmax+" coords:"+coordh.size());

	// NB with hist and dominates duplicated and combined, the walk/sum of coordh/hashc can be
	// parallelized embarassingly as it is readonly.

	// compute score2,per coord. including dominates graph (as it may affect filtering needs)
	Map<Integer,List<Integer>> dominates = computeDominantP(coordh.values(),hashc);

	if (dominates.size() > 0) {
	    int dmax=0;
	    for ( List<Integer> dominated : dominates.values() ){
		if (dominated.size() > dmax){
		    dmax = dominated.size();
		}
	    }

	    System.out.println("there were "+dominates.size()+" dominating coordinates, dmax:"+dmax);


	    List<Map.Entry<Integer,List<Integer>>> es = new ArrayList<>(dominates.size()); // damn you java
	    es.addAll(dominates.entrySet());
	    Comparator<Map.Entry<Integer,List<Integer>>> cmp = Comparator.comparingInt((Map.Entry<Integer,List<Integer>> me)->me.getValue().size()).reversed();
	    es.sort(cmp);
	    
	    for(int i = 0;i < 10; i++){
		System.out.println(es.get(i).getKey()+" = "+es.get(i).getValue().size());
	    }
	    
	} else {
	    System.out.println("there were no dominating coordinates");
	}       	
    }

    /**
     * compute the dominates relation over coords, possibly in parallel
     */
    private static Map<Integer,List<Integer>> computeDominantP(Collection<List<Integer>> coords,Map<Integer,List<Integer>> hashc){
	// chop wood cary water
	// scale/size iterators to match pool count
	int pool = ForkJoinPool.getCommonPoolParallelism();
	int csize = coords.size();
	if (csize < 100000) {
	    pool = 1;
	}
	System.out.println("||"+pool);

	int chunksize= (csize/pool)+1; // yes, int. oversizes, last one will min out

	List<List<Integer>> clist = new ArrayList<>(coords);
	Dominator[] doms = new Dominator[pool];
	for ( int i =0; i < pool; i++ ) {
	    List<List<Integer>> sl = clist.subList(i*chunksize,Math.min((i+1)*chunksize,csize));
	    Iterator<List<Integer>> s = sl.iterator();
	    doms[i] = new Dominator(csize,s,hashc);
	}

	ForkJoinTask.invokeAll(doms);

	Map<Integer,List<Integer>> dominates = new HashMap<>();

	for ( Dominator d : doms ){
	    Map<Integer,List<Integer>> m = d.join();
	    for ( Map.Entry<Integer,List<Integer>> me : m.entrySet() ) {
		List<Integer> l = dominates.get(me.getKey());
		if (l == null){
		    l = new ArrayList<>(me.getValue().size());
		    dominates.put(me.getKey(),l);
		}
		l.addAll(me.getValue());
	    }
	}
	
	// compute score2,per coord. including dominates graph (as it may affect filtering needs)

	return dominates;
    }

    /**
     * worker for concurrent computation of dominate relation over the observation set
     */
    static class Dominator extends RecursiveTask< Map<Integer,List<Integer>> > implements Consumer<List<Integer>> {

	final int[] hist;
	final Map<Integer,List<Integer>> hashc;
	final Iterator<List<Integer>> spl;
	final Map<Integer,List<Integer>> d = new HashMap<>();
	int count =0;
	Dominator(int csize, Iterator<List<Integer>> spl, Map<Integer,List<Integer>> hashc){
	    super();
	    hist = new int[csize+1];
	    this.hashc = hashc;
	    this.spl = spl;
	}

	public void accept(List<Integer> hashes) {
	    count++;
	    int n = hashes.size()-1;
	    if (n < 5 ) {
		return;
	    }
	    if ((count & 0x0ff) == 0){
                System.out.print('.');
            }
	    
            int counti=0;
	    Integer c1=-1;
	    for ( Integer h1 : hashes ) {
		if (counti==0){
		    counti++; // skip the index.
		    c1 = h1;
		    continue;
		}
		List<Integer> hcoords = hashc.get(h1);

		for ( Integer c2 : hcoords ) {
                    counti++;
                    if ((counti & 0x0fffff) == 0 ){
                        System.out.print('@');
                    } 
		    
		    hist[c2]++;
		    
		    if (hist[c2] == n) {
			if ( !c2.equals( c1 ) ){ // c2 dominates c1
			    List<Integer> dominated = d.get(c2);
			    if (null == dominated){
				dominated = new ArrayList<>();
				d.put(c2,dominated);
			    }
			    dominated.add(c1);
			}
		    }
		}
	    }
	    // histogram established. evaluate it for final score
	    // somewhere here.

	    // histogram reset
	    Arrays.fill(hist,0);
	}
	
	@Override
	protected Map<Integer,List<Integer>>  compute(){
	    spl.forEachRemaining(this);
	    System.out.println(count);
	    return d;
	}
    }
    
    
    
    /**
     * read multiple file intermediate m8b data at stageLoc, slice, sort and remove duplicates into reduceLoc
     */
    private static void reduce(String stageLoc, String reduceLoc, int slicebits) throws Exception {
	// first cut:
	//   read each input file
	//     slice and append to a file based on highslice nibble
	//   read each appended file
	//     sort entire file in memory
	//     write out in order skipping dups

	// build output set
	ByteBuffer[] bb = new ByteBuffer[16];
	SeekableByteChannel[] out = new SeekableByteChannel[bb.length];
	Path[] reduce = new Path[bb.length];
	Path[] reduce2 = new Path[bb.length];

	for (int i=0;i<bb.length;i++) {
	    bb[i] = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
	    reduce[i] = new File(reduceLoc,"reduce1_"+i).toPath();
	    reduce2[i] = new File(reduceLoc,"reduce2_"+i).toPath();
	    out[i] = Files.newByteChannel(reduce[i], EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
	}

	int recordsize = 4+9;
	long mask = (1L << slicebits ) - 1;
	long idxmask = (0x0fL << (slicebits-4));
	byte[] mgrs = new byte[9];

	int[] records= new int[bb.length];
	
	ByteBuffer ib = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	try (DirectoryStream<Path> stream = Files.newDirectoryStream(new File(stageLoc).toPath(), "stage_*")) {
           for ( Path entry : stream ) {
	       SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));//,);
	       // 
	       int read = in.read(ib);
	       while (read > 0) {
		   ib.flip();

		   while ( ib.remaining() > recordsize ) {
		       int fullk = ib.getInt();
		       ib.get(mgrs,0,mgrs.length);

		       int key = (int)(fullk & mask);
		       
		       int idx = (int)(((key & idxmask) >> (slicebits-4)) & 0x0f);

		       if (bb[idx].remaining() < recordsize ) {
			   bb[idx].flip();
			   while (bb[idx].hasRemaining()){
			       out[idx].write(bb[idx]);
			   }
			   bb[idx].clear();
		       }

		       bb[idx].putInt(key).put(mgrs);
		       records[idx]++;
		   }
		   ib.compact(); // partial reads.
		   read = in.read(ib);
	       }
           }
	}

	int max = -1;
	// done. do last write/flush
	for (int i=0;i<bb.length;i++) {
	    bb[i].flip();
	    while (bb[i].hasRemaining()){
		out[i].write(bb[i]);
	    }
	    bb[i].clear();
	    
	    out[i].close();

	    if (records[i] > max){
		max = records[i];
	    }
	}


	ib.clear();
	// should we bound this max? secondary option is multifile sortmerge.
	System.out.println("max: "+max+", allocating ~"+(max*(recordsize+8))+"b");
	byte[][] entries = new byte[max][];
	for ( int i = 0;i<entries.length;i++){
	    entries[i] = new byte[recordsize];
	}
	int dups[] = new int[reduce.length];

	for (int i =0;i<reduce.length;i++ ) {
	    Path entry = reduce[i];
	    SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));//,);
	    // 
	    int read = in.read(ib);
	    int idx=0;
	    while (read > 0) {
		ib.flip();
		
		while ( ib.remaining() > recordsize ) {
		    ib.get(entries[idx],0,recordsize); // guaranteed to have recordsize from the conditional and bytebuffer.get will fill what is available.
		    idx++;
		}
		ib.compact(); // partial reads.
		read = in.read(ib);
	    }
	    // sort entries by keybits from 0,idx
	    Arrays.sort(entries,0,idx,CMP);

	    SeekableByteChannel outc = Files.newByteChannel(reduce2[i], EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));
	    ib.clear();
	    int lastwrite=-1;
	    for ( int j = 0; j < idx; j++ ) {
		if (ib.remaining() < recordsize ) {
		    ib.flip();
		    while (ib.hasRemaining()){
			outc.write(ib);
		    }
		    ib.clear();
		}
		// suppress dups
		if ( lastwrite >= 0 ) {
		    if (CMP.compare(entries[lastwrite],entries[j]) == 0) {
			dups[i]++;
			continue;
		    }
		}
		
		ib.put(entries[j],0,recordsize);
		lastwrite = j;
	    }
	    // done. do last write and flush
	    ib.flip();
	    while (ib.hasRemaining()){
		outc.write(ib);
	    }
	    ib.clear();
	    outc.close();
	}
	System.out.println("dups suppressed: "+Arrays.stream(dups).mapToObj(x->Integer.toString(x)).collect(Collectors.joining(", ")));
	
    }


    // entries are LE int32 then MGRS string
    static class RecComparator implements Comparator<byte[]> {
	public int compare(byte[] o1, byte[] o2) {

	    // can't do it all bytewise
	    int o1k = (int)(((o1[3]<<24)&0xff000000)|((o1[2]<<16)&0x00ff0000)|((o1[1]<<8)&0x00ff00)|(o1[0] & 0x0ff));
	    int o2k = (int)(((o2[3]<<24)&0xff000000)|((o2[2]<<16)&0x00ff0000)|((o2[1]<<8)&0x00ff00)|(o2[0] & 0x0ff));

	    int diff=o1k-o2k;
	    
	    if ( diff == 0 ) { // same intk, do pairwise eval
	    for ( int i = 4; i < 13; i++ ) {
		diff = (o1[i]-o2[i]);
		if ( diff != 0 ) {
		    return diff;
		}
	    }
	    }
	    
	    return diff;
	}
    }

    /** reference comparator for records */
    private static Comparator<byte[]> CMP = new RecComparator();


    /**
     * runs reduce for the full keysize inplace on stageLoc, cleans up, renames files
     */
    private static void compact(String stageLoc) throws Exception {
	// starts with stage_
	reduce(stageLoc, stageLoc, 32);

	// now original stage_, reduce_, reduce2_
	// move stage_ to oldstage_, move reduce2_ to stage_, remove oldstage_, remove reduce_,

	for (int i=0;i<16;i++) {
	    Path reduce = new File(stageLoc,"reduce1_"+i).toPath();
	    Path reduce2 = new File(stageLoc,"reduce2_"+i).toPath();
	    Path stage = new File(stageLoc,"stage_"+i).toPath();
	    Path oldstage = new File(stageLoc,"oldstage_"+i).toPath();

	    Files.move(stage,oldstage);
	    Files.move(reduce2,stage);

	    Files.delete(oldstage);
	    Files.delete(reduce);
	}
	
    }
    
    /**
     * read multiple file reduced intermediate m8b data at reduceLoc, combine into final m8b file at toFile
     */
    private static void combine(String reduceLoc, String toFile, int slicebits) throws Exception {
	int recordsize = 4+9;
	Path reduce[] = new Path[16];
	long total = 0;
	for (int i=0;i<reduce.length;i++) {
	    reduce[i] = new File(reduceLoc,"reduce2_"+i).toPath();
	    total += Files.size(reduce[i]);
	}
	int records = (int)(total / recordsize);
	
	Charset utf8  = Charset.forName("UTF-8");
	SeekableByteChannel out = Files.newByteChannel(new File(toFile).toPath(), EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE));//,);

         
        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
    
	// write header
	bb.put("MJG\n".getBytes(utf8)); // magic number
	bb.put("2\n".getBytes(utf8)); // version
	bb.put("SIP-2-4\n".getBytes(utf8)); // hash
	bb.put(String.format("%x\n",slicebits).getBytes(utf8)); // slice bits (hex)
	bb.put("MGRS-1000\n".getBytes(utf8)); // coords
	bb.put("4\n".getBytes(utf8)); // id size in bytes (hex)
	bb.put("9\n".getBytes(utf8)); // coords size in bytes (hex)
	bb.put(String.format("%x\n",records).getBytes(utf8)); // record count (hex)

	bb.flip();
	while (bb.hasRemaining()){
	    out.write(bb);
	}

	// back to fill mode
	bb.clear();


	for (int i = 0; i < reduce.length; i++){
	    Path entry = reduce[i];
	    SeekableByteChannel in = Files.newByteChannel(entry, EnumSet.of(StandardOpenOption.READ));
	    
	    int read = in.read(bb);
	    int idx=0;
	    while (read > 0) {
		bb.flip();
		while (bb.hasRemaining()){
		    out.write(bb);
		}
		bb.clear();
		read = in.read(bb);
	    }
	}
	bb.flip();
	while (bb.hasRemaining()){
	    out.write(bb);
	}
	out.close();
    }

    private static void dumpi(String inFile) throws Exception {
	int recordsize = 4+9;
	Charset utf8  = Charset.forName("UTF-8");

	SeekableByteChannel in = Files.newByteChannel(new File(inFile).toPath(), EnumSet.of(StandardOpenOption.READ));
	
        
        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	
	int read = in.read(bb);
	byte[] tmp = new byte[256];
	int mgrsize = 9;
	while (read > 0) {
	    bb.flip();
	    while ( bb.remaining() > recordsize ) {
		int id =  bb.getInt();

		bb.get(tmp,0,mgrsize);
		String mgrs = new String(tmp,0,mgrsize,utf8);
		System.out.println(Integer.toHexString(id)+" : "+mgrs);
	    }
	    bb.compact();
	    read = in.read(bb);
	}
	
    }


    
    // 8e:15:44:60:50:ac -> [0x8e, 0x15, 0x44, 0x60, 0x50, 0xac] -> hash masked off at n-th LS bit
    /** 
     * read mac from text string into macbytes. run siphahsh(skipkeky,macbytes) and mask  
     * the low-n bits (n=10 would only produce integer less than 1024)
     */
    private static Integer extractKeyFrom(String mac, byte[] macbytes, SipKey sipkey, int n) {
	Integer kslice2 = Integer.valueOf( extractIntKeyFrom(mac,macbytes,sipkey,n) );
	return kslice2;
    }

    /** 
     * read mac from text string into macbytes. run siphahsh(skipkeky,macbytes) and mask  
     * the low-n bits (n=10 would only produce integer less than 1024)
     */
    private static int extractIntKeyFrom(String mac, byte[] macbytes, SipKey sipkey, int n) {

	for ( int i = 0; i < macbytes.length; i++ ) {
	    char hi = mac.charAt(i*3);
	    char lo = mac.charAt(i*3+1);
	    byte hib = (byte) ((nybbleFrom(hi) << 4) & 0xf0);
	    byte lob = nybbleFrom(lo);
	    macbytes[i] = (byte)( hib | lob);
	}
	
	long siph = SipHash.digest(sipkey, macbytes);

	long mask = (1L << n ) - 1;

	return (int)(siph & mask);
    }

    /**
     * return the nybble value of hex char c
     */
    private static byte nybbleFrom(char c){
	switch(c){
	case '0':
	case '1':
	case '2':
	case '3':
	case '4':
	case '5':
	case '6':
	case '7':
	case '8':
	case '9':
	    return (byte)(c - '0');
	case 'A':
	case 'a':
	    return 10;
	case 'B':
	case 'b':
	    return 11;
	case 'C':
	case 'c':
	    return 12;
	case 'D':
	case 'd':
	    return 13;
	case 'E':
	case 'e':
	    return 14;
	case 'F':
	case 'f':
	    return 15;
	default:
	    throw new IllegalArgumentException("non hex char '"+c+"'");
	}
    }

    /**
     * read all of m8bFile into a data structure, then query against it for macs.
     * query algo looks like:
     *
     	  map&lt;coords,int%gt; sum

	  foreach mac in macs
	    digest h = hash(mac)
	    bits id = truncate(h,n)
	    set&lt;coords&gt; s = map.get(id)
	    // histogram the results
            foreach c in s
	      sum[c]++
	  
	  // larger sums = more likely coords
     *
     */
    private static void query(String m8bFile, String[] macs) throws Exception {
	// rebuild m8b state from file:

	Map<Integer,List<byte[]>> mjg = new TreeMap<Integer,List<byte[]>>();

	Charset utf8  = Charset.forName("UTF-8");
	SeekableByteChannel in = Files.newByteChannel(new File(m8bFile).toPath(), EnumSet.of(StandardOpenOption.READ));//,);

        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	int read = in.read(bb);
	boolean header = true;
	byte[] tmp = new byte[256];
	int recordsize=-1;
	int slicebits=0;
	int mgrsize =0;
	
	while (read > 0) {
	    bb.flip();
	    if (header){
		// read header
		bb.get(tmp,0,tmp.length);
		int offset=-1;
		int linecount=0;
		for ( int i = 0; i < tmp.length; i++ ) {
		    if (tmp[i] == (byte)'\n') {
			linecount++;
			if ( linecount == 8 ) {
			    offset = i;
			    break;
			}
		    }
		}
		if (offset < 8) {
		    System.out.println("malformed header");
		    return;
		}
		String[] s = new String(tmp,0,offset,utf8).split("\n");
		if (!"MJG".equals(s[0])){
		    System.out.println("bad MaJGic");
		    return;
		}
		int vers = Integer.parseInt(s[1]);
		if (vers != 2) {
		    System.out.println("unsupported version:"+vers);
		    return;
		}
		if (!"SIP-2-4".equals(s[2])){
		    System.out.println("unsupported hash:"+s[2]);
		    return;
		}
		
		slicebits = Integer.parseInt(s[3],16);

		if (!"MGRS-1000".equals(s[4])){
		    System.out.println("unsupported coords:"+s[4]);
		    return;
		}
		
		mgrsize = Integer.parseInt(s[6],16);
		recordsize = (Integer.parseInt(s[5],16)+ mgrsize);
		bb.position(offset+1);
		header = false;
	    }
	    while ( bb.remaining() > recordsize ) {
		Integer kslice2 = Integer.valueOf( bb.getInt() );
		byte[] mgrs = new byte[mgrsize]; // don't hold onto bloaty strings when we don't need to
		bb.get(mgrs,0,mgrsize);
		
		List<byte[]> locs = mjg.get(kslice2);
		if (locs==null){
		    locs = new ArrayList<byte[]>();
		    mjg.put(kslice2,locs);
		}
		locs.add(mgrs);
	    }
	    bb.compact(); // partial reads.
	    read = in.read(bb);
	}
	
	System.out.println("loaded "+mjg.size());

	// execute query
	SipKey sipkey = new SipKey(new byte[16]);
	byte[] macbytes = new byte[6];

	Map<String,int[]> lochist = new HashMap<String,int[]>();
	
	for ( String mac : macs ) {
	    Integer kslice2 = extractKeyFrom(mac, macbytes, sipkey, slicebits);

	    List<byte[]> locs = mjg.get(kslice2);
	    if (locs==null) {
		continue;
	    }

	    for ( byte[] locb : locs ) {
		String loc = new String(locb,0,locb.length,utf8);
		int[] val = lochist.get(loc);
		if ( val == null ) {
		    val = new int[]{0};
		    lochist.put(loc,val);
		}
		val[0]++;
	    }
	}

	List<Map.Entry<String,int[]>> es = new ArrayList<>(lochist.size()); // damn you java
	es.addAll(lochist.entrySet());
        Comparator<Map.Entry<String,int[]>> cmp = Comparator.comparingInt((Map.Entry<String,int[]> me)->me.getValue()[0]).reversed();
	es.sort(cmp);

	es.forEach((me)->System.out.printf("%s %d\n",me.getKey(),me.getValue()[0]));
    }


    /**
     * read just enough of m8bFile to build minimal state to query, by filtering hard on macs
     * Then query against it for macs.
     * query algo looks like:
     *
     	  map&lt;coords,int%gt; sum

	  foreach mac in macs
	    digest h = hash(mac)
	    bits id = truncate(h,n)
	    set&lt;coords&gt; s = map.get(id)
	    // histogram the results
            foreach c in s
	      sum[c]++
	  
	  // larger sums = more likely coords
     *
     */
    private static void scan(String m8bFile, String[] macs) throws Exception {
	// rebuild minimal m8b state from file by filtering hard on macs:

	SipKey sipkey = new SipKey(new byte[16]);
	byte[] macbytes = new byte[6];

	Set<Integer> keyset = new HashSet<Integer>();
	List<Integer> keyring = new ArrayList<Integer>(macs.length);

	Integer maxkey = Integer.valueOf(Integer.MIN_VALUE);
		
	Map<Integer,List<String>> mjg = new TreeMap<Integer,List<String>>();

	Charset utf8  = Charset.forName("UTF-8");
	ReadableByteChannel in = Files.newByteChannel(new File(m8bFile).toPath(), EnumSet.of(StandardOpenOption.READ));//,);

	if (m8bFile.endsWith(".gz") || m8bFile.endsWith(".GZ")) {// there's got to be a better way!
	    InputStream is = Channels.newInputStream(in);
	    GZIPInputStream gis = new GZIPInputStream(is);
	    in = Channels.newChannel(gis);
	}
	
        ByteBuffer bb = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN); // screw you, java
	int read = in.read(bb);
	boolean header = true;
	byte[] tmp = new byte[256];
	int recordsize=-1;
	int slicebits=0;
	int mgrsize =0;

	int lastid = -1;
	Integer lastkey=null;
	int maxid = maxkey.intValue();

	boolean done = false;
	
	while (read > 0 && !done) {
	    bb.flip();
	    if (header){
		// read header
		bb.get(tmp,0,tmp.length);
		int offset=-1;
		int linecount=0;
		for ( int i = 0; i < tmp.length; i++ ) {
		    if (tmp[i] == (byte)'\n') {
			linecount++;
			if ( linecount == 8 ) {
			    offset = i;
			    break;
			}
		    }
		}		
		if (offset < 8) {
		    System.out.println("malformed header");
		    return;
		}
		String[] s = new String(tmp,0,offset,utf8).split("\n");
		if (!"MJG".equals(s[0])){
		    System.out.println("bad MaJGic");
		    return;
		}
		int vers = Integer.parseInt(s[1]);
		if (vers != 2) {
		    System.out.println("unsupported version:"+vers);
		    return;
		}
		if (!"SIP-2-4".equals(s[2])){
		    System.out.println("unsupported hash:"+s[2]);
		    return;
		}
		
		slicebits = Integer.parseInt(s[3],16);

		if (!"MGRS-1000".equals(s[4])){
		    System.out.println("unsupported coords:"+s[4]);
		    return;
		}
		
		mgrsize = Integer.parseInt(s[6],16);
		recordsize = (Integer.parseInt(s[5],16)+ mgrsize);

		bb.position(offset+1);
		header = false;


		for ( String mac : macs ) {
		    Integer kslice2 = extractKeyFrom(mac, macbytes, sipkey, slicebits);
		    keyset.add(kslice2);
		    keyring.add(kslice2);
		    if (kslice2.intValue() > maxkey.intValue() ){
			maxkey = kslice2;
		    }
		}
		maxid = maxkey.intValue();
		System.out.println("maxid "+maxid);
	    }
	    while ( bb.remaining() > recordsize ) {
		int id =  bb.getInt();
		if ( id < lastid ) {
		    System.out.println(" disorder! read "+id+" < "+lastid);
		}
		if (id > maxid ) {
		    System.out.println(id +" >  "+maxid);
		    // nothing more to see folks,
		    done = true;
		    break;
		}
		Integer kslice2 = null;
		if ( id == lastid ) { // same as it was
		    // are we still in a key range?
		    if ( lastkey != null ) {
			kslice2 = lastkey;
		    } else {
			// advance the buffer, check the next entry
			bb.position(bb.position()+mgrsize);
			continue;
		    }
		} else {
		    // new id. check things out.
		    lastid = id;
		    kslice2 = Integer.valueOf( id );
		    if (keyset.contains(kslice2)){
			lastkey = kslice2;
		    } else {
			lastkey = null;
			// advance the buffer, check the next entry
			bb.position(bb.position()+mgrsize);			
			continue;
		    }
		}

		bb.get(tmp,0,mgrsize);
		String mgrs = new String(tmp,0,mgrsize,utf8);

		List<String> locs = mjg.get(kslice2);
		if (locs==null){
		    locs = new ArrayList<String>();
		    mjg.put(kslice2,locs);
		}
		locs.add(mgrs);
	    }
	    bb.compact(); // partial reads.
	    read = in.read(bb);
	}
	
	System.out.println("loaded "+mjg.size());

	// execute query
	Map<String,int[]> lochist = new HashMap<String,int[]>();
	
	for ( Integer kslice2 : keyring ) {

	    List<String> locs = mjg.get(kslice2);
	    if (locs==null) {
		continue;
	    }

	    for ( String loc : locs ) {
		int[] val = lochist.get(loc);
		if ( val == null ) {
		    val = new int[]{0};
		    lochist.put(loc,val);
		}
		val[0]++;
	    }
	}

	List<Map.Entry<String,int[]>> es = new ArrayList<>(lochist.size()); // damn you java
	es.addAll(lochist.entrySet());
        Comparator<Map.Entry<String,int[]>> cmp = Comparator.comparingInt((Map.Entry<String,int[]> me)->me.getValue()[0]).reversed();
	es.sort(cmp);

	es.forEach((me)->System.out.printf("%s %d\n",me.getKey(),me.getValue()[0]));
    }
}
