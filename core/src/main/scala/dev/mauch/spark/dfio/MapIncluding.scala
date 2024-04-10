package dev.mauch.spark.dfio

case class MapIncluding[K](keys: Seq[K], optionally: Seq[K] = Seq()) {
  def unapply[V](m: Map[K, V]): Option[(Seq[V], Seq[Option[V]])] =
    if (keys.forall(m.contains)) Some((keys.map(m), optionally.map(m.get)))
    else None
}
