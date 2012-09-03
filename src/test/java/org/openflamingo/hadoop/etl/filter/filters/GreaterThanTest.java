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
import org.openflamingo.hadoop.etl.filter.filters.GreaterThan;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */

@RunWith(MockitoJUnitRunner.class)
public class GreaterThanTest {
	private FilterClass greaterThan;
	@Mock
	private FilterModel filterModel;

	@Before
	public void setUp() throws Exception {
		greaterThan = new GreaterThan();
	}

	@Test
	public void testDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("1");

		boolean success = greaterThan.doFilter("10", filterModel);

		assertThat(success, is(true));
	}
	@Test
	public void testFailDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("10");

		boolean success = greaterThan.doFilter("1", filterModel);

		assertThat(success, is(false));
	}
	@Test(expected = NumberFormatException.class)
	public void testExceptionFilter(){
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("10");

		boolean success = greaterThan.doFilter("data", filterModel);
	}
}
