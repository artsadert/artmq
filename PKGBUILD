pkgname=artmq
pkgver=0.1.0
pkgrel=1
pkgdesc="Lightweight MQTT message broker with priority queue and TTL"
arch=('x86_64')
url="https://github.com/artsadert/artmq"
license=('MIT')
depends=()
makedepends=('go')
source=("$pkgname::git+$url.git")
sha256sums=('SKIP')

build() {
  cd "$pkgname"
  go build -o artmq ./cmd/artmq/main.go
}

package() {
  cd "$pkgname"
  install -Dm755 artmq "$pkgdir/usr/bin/artmq"
}

post_install() {
  install -Dm644 artmq.service "$pkgdir/usr/lib/systemd/system/artmq.service"
}
