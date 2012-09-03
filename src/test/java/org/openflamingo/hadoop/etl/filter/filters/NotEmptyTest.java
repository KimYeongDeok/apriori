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
import org.openflamingo.hadoop.etl.filter.filters.NotEmpty;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
@RunWith(MockitoJUnitRunner.class)
public class NotEmptyTest {
	private FilterClass notEmpty;
	@Mock
	private FilterModel filterModel;

	@Before
	public void setUp() throws Exception {
		notEmpty = new NotEmpty();
	}

	@Test
	public void testDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("");

		boolean success = notEmpty.doFilter("data",filterModel);

		assertThat(success, is(true));
	}
	@Test
	public void testFailDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("");

		boolean success = notEmpty.doFilter("", filterModel);

		assertThat(success, is(false));
	}
}
