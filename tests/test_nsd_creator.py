from new_subdomain.nsd_creator import extract_subdomain

import pytest

@pytest.mark.parametrize('fqdn, core_domain, max_subdomain_depth, expected_subdomain', [
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 1, 'g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 2, 'f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 3, 'e.f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 4, 'd.e.f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 5, 'c.d.e.f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 6, 'b.c.d.e.f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 7, 'a.b.c.d.e.f.g.h.i.com'),
    ('a.b.c.d.e.f.g.h.i.com', 'h.i.com', 1000, 'a.b.c.d.e.f.g.h.i.com')
])
def test_extract_subdomains(fqdn, core_domain, max_subdomain_depth, expected_subdomain):
    subdomain = extract_subdomain(fqdn, core_domain, max_subdomain_depth)
    assert subdomain == expected_subdomain