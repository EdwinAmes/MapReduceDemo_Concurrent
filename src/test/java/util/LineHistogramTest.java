package util;

import com.ps.mapreducedemo.util.LineHistogramMaker;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Created by Edwin on 4/20/2016.
 */
public class LineHistogramTest {

    @Before
    public void setup()
    {
        sut = new LineHistogramMaker();
    }

    private LineHistogramMaker sut;

    @Test
    public void when_given_punctuation_strips_it_out(){
        Map<String, Long> result = sut.getHistogramForLine("!@#$%^&**()-+.,;:[]{}|\\/?<>~`=\"");
        assertThat(result.size(), is(equalTo(0)));
    }

    @Test
    public void when_given_empty_string_returns_empty_map()
    {
        Map<String, Long> result = sut.getHistogramForLine("");
        assertThat(result.size(), is(equalTo(0)));
    }

    @Test
    public void when_given_blank_string_returns_empty_map()
    {
        Map<String, Long> result = sut.getHistogramForLine("                         ");
        assertThat(result.size(), is(equalTo(0)));
    }

    @Test
    public void when_given_string_with_one_word_returns_correct_map(){
        Map<String, Long> result = sut.getHistogramForLine("     abc          ");
        assertThat(result.size(), is(equalTo(1)));
        assertThat(result.get("abc"), is(equalTo(1L)));
    }

    @Test
    public void when_given_string_with_three_words_returns_correct_map(){
        Map<String, Long> result = sut.getHistogramForLine("     abc   1243    doe    ");
        assertThat(result.size(), is(equalTo(3)));
        assertThat(result.get("abc"), is(equalTo(1L)));
        assertThat(result.get("1243"), is(equalTo(1L)));
        assertThat(result.get("doe"), is(equalTo(1L)));
    }

    @Test
    public void when_given_string_with_three_words_with_a_dupe_returns_correct_map(){
        Map<String, Long> result = sut.getHistogramForLine("     abc   1243    abc    ");
        assertThat(result.size(), is(equalTo(2)));
        assertThat(result.get("abc"), is(equalTo(2L)));
        assertThat(result.get("1243"), is(equalTo(1L)));
    }
}
