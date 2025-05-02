'use client';

import { useState, useEffect } from 'react';
import { Post } from '@/lib/types';
import { getUserFeed } from '@/lib/api';
import { PostCard } from '@/components/post-card';
import { Skeleton } from '@/components/ui/skeleton';
import { Button } from '@/components/ui/button';

interface UserFeedProps {
  userId: string;
}

export function UserFeed({ userId }: UserFeedProps) {
  const [feed, setFeed] = useState<Post[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processedFeed, setProcessedFeed] = useState<Post[]>([]);

  const fetchFeed = async () => {
    if (!userId) return;

    try {
      setLoading(true);
      const feedData = await getUserFeed(userId);
      setFeed(feedData.posts);
    } catch (err) {
      setError('Failed to load feed');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  // Process feed to avoid showing shared posts and their originals at the same level
  // and filter out comments that are displayed as posts
  useEffect(() => {
    if (feed.length === 0) return;

    // Find all parent post IDs from shared posts
    const sharedPostParentIds = new Set<string>();
    feed.forEach(post => {
      if (post.post_type === 'SHARE' && post.parent_post_id) {
        sharedPostParentIds.add(post.parent_post_id);
      }
    });

    // Filter out posts that are already shown as shared content
    // and filter out comments (they should be attached to posts, not shown separately)
    const filtered = feed.filter(post => {
      // Comments might be included in the feed but should be displayed attached to posts
      // Look for characteristics of comments like parent_post_id existing but not being a SHARE type
      const isComment = 
        post.parent_post_id && 
        post.post_type !== 'SHARE' && 
        (post.post_type === 'TEXT' || post.content.length < 300); // Heuristic: comments are usually shorter
      
      // Keep only if it's not already displayed as shared content or if it's a share wrapper itself
      const isNotDuplicate = !sharedPostParentIds.has(post.post_id) || post.post_type === 'SHARE';
      
      return !isComment && isNotDuplicate;
    });

    // Sort by creation date, newest first
    filtered.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());

    setProcessedFeed(filtered);
  }, [feed]);

  useEffect(() => {
    fetchFeed();
  }, [userId]);

  if (loading) {
    return (
      <div className="space-y-4">
        {[1, 2, 3].map((i) => (
          <div key={i} className="rounded-lg border p-4">
            <div className="flex items-center space-x-3">
              <Skeleton className="h-10 w-10 rounded-full" />
              <div className="space-y-2">
                <Skeleton className="h-4 w-[200px]" />
                <Skeleton className="h-3 w-[150px]" />
              </div>
            </div>
            <div className="mt-4 space-y-2">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-3/4" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border bg-red-50 p-4 text-red-500">
        Error: {error}
        <Button 
          onClick={fetchFeed} 
          variant="outline" 
          size="sm" 
          className="ml-2"
        >
          Retry
        </Button>
      </div>
    );
  }

  if (processedFeed.length === 0) {
    return (
      <div className="rounded-lg border bg-gray-50 p-8 text-center">
        <p className="text-gray-500">No posts in your feed yet.</p>
        <p className="text-sm text-gray-400">Follow more users to see their posts here.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between">
        <h2 className="text-xl font-bold">Your Feed</h2>
        <Button onClick={fetchFeed} variant="outline" size="sm">
          Refresh
        </Button>
      </div>
      <div className="space-y-4">
        {processedFeed.map((post) => (
          <PostCard 
            key={post.post_id} 
            post={post} 
            currentUserId={userId} 
            onRefresh={fetchFeed}
          />
        ))}
      </div>
    </div>
  );
} 