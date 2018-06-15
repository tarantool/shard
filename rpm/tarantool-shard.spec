Name: tarantool-shard
Version: 1.1.0
Release: 1%{?dist}
Summary: Tarantool sharding module
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/shard
Source0: https://github.com/tarantool/shard/archive/%{version}/shard-%{version}.tar.gz
BuildRequires: cmake >= 2.8
BuildRequires: gcc >= 4.5
BuildRequires: tarantool >= 1.7.2.0
BuildRequires: tarantool-devel
BuildRequires: msgpuck-devel >= 1.0.0
Requires: tarantool >= 1.7.2.0

# For tests
%if (0%{?fedora} >= 22)
BuildRequires: python >= 2.7
BuildRequires: python-six >= 1.9.0
BuildRequires: python-gevent >= 1.0
BuildRequires: python-yaml >= 3.0.9
# Temporary for old test-run
# https://github.com/tarantool/shard/issues/1
BuildRequires: python-daemon
%endif

%description
An application-level library that provides sharding and client-side reliable
replication for tarantool 1.6. Implements a single-phase and two-phase
protocol operations (with batching support), monitors availability of nodes
and automatically expells failed nodes from the cluster.

%prep
%setup -q -n shard-%{version}

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make %{?_smp_mflags}

# tests are disabled till they are fixed (see https://github.com/tarantool/shard/issues/71)
# %check
# %if (0%{?fedora} >= 22)
# make test
# %endif

%install
%make_install

%files
%{_libdir}/tarantool/shard/
%{_datarootdir}/tarantool/shard/
%doc README.md
%{!?_licensedir:%global license %doc}
%license LICENSE

%changelog
* Fri Feb 19 2016 Roman Tsisyk <roman@tarantool.org> 1.1.0-1
- Initial version of the RPM spec
