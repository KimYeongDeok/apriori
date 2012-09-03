package org.openflamingo.hadoop.etl.filter.filters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.openflamingo.hadoop.etl.filter.FilterModel;
import org.openflamingo.hadoop.etl.filter.filters.Empty;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */

@RunWith(MockitoJUnitRunner.class)
public class EmptyTest {
	private Empty empty;
	@Mock
	private FilterModel filterModel;

	@Before
	public void before(){
		empty = new Empty();
	}
	@Test
	public void testDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);

		boolean success = empty.doFilter("", filterModel);

		assertThat(success, is(true));
	}

	@Test
	public void testException(){
		when(filterModel.getColumnIndex()).thenReturn(10);

		boolean success = empty.doFilter("data", filterModel);

		assertThat(success, is(false));
	}
}
