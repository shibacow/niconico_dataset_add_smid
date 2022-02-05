package com.shibacow.nico.common;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.JapaneseTokenizer;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.BaseFormAttribute;
import org.codelibs.neologd.ipadic.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;

public class Kuromoji {
    private static final Logger LOG = LoggerFactory.getLogger(Kuromoji.class);

    /**
     * 分かち書きした文章をリストにして返す。
     * 
     * @param src
     * @return
     */
    public List<String> kuromojineologd(final String src){
        final List<String> ret = new ArrayList<>();
        try(final JapaneseTokenizer jt = 
                // JapaneseTokenizerの引数は(ユーザー辞書, 記号を無視するか, モード)
                new JapaneseTokenizer(null, false, JapaneseTokenizer.Mode.NORMAL)){
            final CharTermAttribute ct = jt.addAttribute(CharTermAttribute.class);
            final BaseFormAttribute base = jt.addAttribute(BaseFormAttribute.class);
            final PartOfSpeechAttribute partof = jt.addAttribute(PartOfSpeechAttribute.class);

            jt.setReader(new StringReader(src));
            jt.reset();
            while(jt.incrementToken()){
                String partsOf = partof.getPartOfSpeech();
                String partsOfs = partsOf.split("-")[0];
                final String word = base.getBaseForm() != null ? base.getBaseForm() : ct.toString();
                final String key = partsOfs + "_" + word;
                ret.add(key);
            }
            jt.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return ret;
    }
}