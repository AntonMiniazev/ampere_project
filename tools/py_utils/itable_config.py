"""Shared ITables display settings for Ampere notebooks."""

from __future__ import annotations

import itables
from itables import JavascriptCode

show = itables.show


def configure_itables() -> None:
    """Use compact tables with global search, column filters, and grouped digits."""
    itables.init_notebook_mode(all_interactive=False)
    itables.options.column_filters = "header"
    itables.options.layout = {
        "top1": "searchBuilder",
        "topStart": "pageLength",
        "topEnd": "search",
        "bottomStart": "info",
        "bottomEnd": "paging",
    }
    itables.options.pageLength = 25
    itables.options.scrollX = True
    itables.options.maxBytes = 1_000_000
    itables.options.showIndex = False
    itables.options.classes = "compact"
    itables.options.css = """
    .dt-container,
    .dt-container table.dataTable,
    .dt-container table.dataTable > thead > tr,
    .dt-container table.dataTable > tbody > tr,
    .dt-container table.dataTable > tfoot > tr,
    .dt-container table.dataTable :is(th, td) {
        background: transparent !important;
        box-shadow: none !important;
        color: inherit;
    }
    """
    itables.options.columnDefs = [{
        "targets": "_all",
        "render": JavascriptCode(
            "function(data, type) {"
            "if (type === 'display' && typeof data === 'number') "
            "return new Intl.NumberFormat('en-US', {maximumFractionDigits: 0}).format(data);"
            "return data;"
            "}"
        ),
    }]
