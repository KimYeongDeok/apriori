package org.openflamingo.hadoop.etl.filter.filters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;
import org.openflamingo.hadoop.etl.filter.filters.LeastThanEquals;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
@RunWith(MockitoJUnitRunner.class)
public class LeastThanEqualsTest {
	private FilterClass leastThanEquals;
	@Mock
	private FilterModel filterModel;

	@Before
	public void setUp() throws Exception {
		leastThanEquals = new LeastThanEquals();
	}

	@Test
	public void testDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);

		when(filterModel.getTerms()).thenReturn("2");
		boolean success = leastThanEquals.doFilter("1",filterModel);
		assertThat(success, is(true));

		when(filterModel.getTerms()).thenReturn("2");
		success = leastThanEquals.doFilter("2",filterModel);
		assertThat(success, is(true));
	}
	@Test
	public void testFailDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("1");

		boolean success = leastThanEquals.doFilter("10", filterModel);

		assertThat(success, is(false));
	}
	@Test(expected = NumberFormatException.class)
	public void testExceptionFilter(){
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("10");

		boolean success = leastThanEquals.doFilter("data", filterModel);
	}
}
