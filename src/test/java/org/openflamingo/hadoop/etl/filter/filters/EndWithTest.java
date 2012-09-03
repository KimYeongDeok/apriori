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
import org.openflamingo.hadoop.etl.filter.filters.EndWith;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
@RunWith(MockitoJUnitRunner.class)
public class EndWithTest {

	@Mock
	private FilterModel filterModel;
	private FilterClass endWith;


	@Before
	public void before(){
		endWith = new EndWith();
	}

	@Test
	public void testDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("df");

		boolean success = endWith.doFilter("asdf", filterModel);

		assertThat(success, is(true));
	}
	@Test
	public void testFailDoFilter() throws Exception {
		when(filterModel.getColumnIndex()).thenReturn(0);
		when(filterModel.getTerms()).thenReturn("as");

		boolean success = endWith.doFilter("asdf", filterModel);

		assertThat(success, is(false));
	}
}
